import Base64 from 'js-base64';
import path from 'node:path';
import consola from 'consola';
import os from 'node:os';
import {
  appendToLocalFile,
  createRecursiveFolder,
  deleteFile,
  generateFilePath,
  getLocalFiles,
  getInitDateTime,
  setInitDateTime,
} from './utils';
import { compressFile, copyGz } from './zip';
import { copyGzFromS3Redshift, filesToS3 } from './S3Handle';
import { sendMessageToQueue } from './sqs';
import { influxdb } from './metrics';
import { LIMIT_RECORDS, LIMIT_SECONDS } from './constants/constants';
import { convertHrtime } from './convertHrtime';
import { IntervalTime } from './constants/intervalTime';

const computerName = os.hostname();

const localPath: string = `${process.cwd()}/${process.env.FOLDER_LOCAL}` || '';
consola.info(`FOLDER_LOCAL:${localPath}`);

const affiliateIdsUnique = new Set();

const sendToAffIdsToSqs = async () => {
  try {
    const uniques = Array.from(affiliateIdsUnique);
    if (uniques.length === 0) return;

    const messageBody = {
      body: JSON.stringify({
        type: 'traffic',
        affiliatesId: uniques,
        timestamp: Date.now(),
      }),
    };

    consola.info(`Added to SQS  Body:${JSON.stringify(messageBody)}`);
    const sqsData = await sendMessageToQueue(messageBody);
    influxdb(200, 'send_to_affIds_to_sqs_success');
    consola.info(`sqsData:${JSON.stringify(sqsData)}`);
    affiliateIdsUnique.clear();
  } catch (e) {
    influxdb(500, 'send_to_affIds_to_sqs_error');
    consola.error('sendToAffIdsToSqsError:', e);
  }
};

setInterval(sendToAffIdsToSqs, IntervalTime.SEND_AFFILIATES_IDS_TO_SQS);

const filesGzToS3 = async (): Promise<string[]> => {
  try {
    const localFolder: string = `${process.env.FOLDER_LOCAL}_gz` || '';
    const files = await getLocalFiles(localFolder);
    // consola.info(`gz files:${JSON.stringify(files)}`)
    if (files.length === 0) {
      consola.info(`[SECOND_STEP_FILES_TO_S3_NO_FILES] FilesGzToS3 no zip files at: ${localFolder}`);
      return [];
    }
    await filesToS3(files);
    return files;
  } catch (e) {
    influxdb(500, 'file_gz_processing_error');
    consola.error('fileGzProcessingError:', e);
    return [];
  }
};

export const aggregateDataProcessing = async (aggregationObject: object) => {
  const currentTime: number = Math.floor((new Date().getTime()) / 1000);

  const countRecords: number = Object.keys(aggregationObject).length;
  const secondLeft = currentTime - getInitDateTime()!;
  if (countRecords >= 1 && secondLeft > LIMIT_SECONDS) {
    consola.info(`[INIT_PROCESSING] Records:${countRecords} LIMIT_RECORDS:${LIMIT_RECORDS},  seconds have passed:${secondLeft}, LIMIT_SECONDS:${LIMIT_SECONDS}, computerName:{ ${computerName} }`);
  }

  // if (secondLeft >= LIMIT_SECONDS
  //   && countRecords >= 1
  // ) {
  //   consola.info(`ComputerName:${computerName}, pass ${LIMIT_SECONDS} seconds with records count:${countRecords}, process at event we have only one records`)
  // }

  if (countRecords >= LIMIT_RECORDS
    || (
      countRecords >= 1
      && secondLeft >= LIMIT_SECONDS
    )
  ) {
    try {
      const startTime: bigint = process.hrtime.bigint();
      const lids: Array<string> = [];
      let records = '';
      for (const [key, value] of Object.entries(aggregationObject)) {
        const buffer = JSON.parse(Base64.decode(key));
        buffer.click = value.count;
        const timeCurrent: number = new Date().getTime();
        affiliateIdsUnique.add(buffer.affiliate_id);
        lids.push(buffer.lid);
        buffer.date_added = Math.floor(timeCurrent / 1000);
        // buffer.event = `${buffer.event}-${computerName}`;
        records += `${JSON.stringify(buffer)}\n`;
      }
      const recordsReady = records.slice(0, -1);
      consola.info(`[FIRST_STEP_LIDS_COUNT] Lids count: { ${lids.length} }. Lids list:${lids}, computerName:{ ${computerName} }`);
      // influxdb(200, `lids_pool_${computerName}_count_${lids.length}`)
      // @ts-ignore
      // eslint-disable-next-line no-param-reassign
      Object.keys(aggregationObject).forEach((k) => delete aggregationObject[k]);
      setInitDateTime(null);
      const filePath = generateFilePath(localPath) || '';
      const fileFolder = path.dirname(filePath);
      await createRecursiveFolder(fileFolder);
      await appendToLocalFile(filePath, recordsReady);
      await compressFile(filePath);
      await copyGz(filePath);
      await deleteFile(filePath);
      const endTime: bigint = process.hrtime.bigint();
      const diffTime: bigint = endTime - startTime;
      consola.success(`[FIRST_STEP_CREATE_FILE_SUCCESS] time processing: { ${convertHrtime(diffTime)} } ms, create local gz file:${filePath} computerName:{ ${computerName} }`);
      const files: string[] = await filesGzToS3();
      await copyGzFromS3Redshift(files!);
    } catch (e) {
      influxdb(500, 'aggregate_data_processing_error');
      consola.error('error generate zip file:', e);
    }
  }
};
