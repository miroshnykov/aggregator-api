import Base64 from "js-base64";
import {
  appendToLocalFile,
  createRecursiveFolder,
  deleteFile,
  generateFilePath,
  getLocalFiles,
  getInitDateTime,
  setInitDateTime
} from "./utils";
import path from "path";
import {compressFile, copyGz} from "./zip";
import consola from "consola";
import {copyZipFromS3Redshift, filesToS3} from "./S3Handle";
import {createDeflateRaw} from "zlib";
import {sendMessageToQueue} from "./sqs"
import {influxdb} from "./metrics";
import os from "os"
import {LIMIT_RECORDS, LIMIT_SECONDS} from "./constants";

const computerName = os.hostname()

const localPath: string = `${process.cwd()}/${process.env.FOLDER_LOCAL}` || ''
consola.info(`FOLDER_LOCAL:${localPath}`)

const affiliateIdsUnique = new Set();

const sendToAffIdsToSqs = async () => {
  try {
    const uniques = Array.from(affiliateIdsUnique)
    if (uniques.length === 0) return

    const messageBody = {
      body: JSON.stringify({
        type: 'traffic',
        affiliatesId: uniques,
        timestamp: Date.now()
      })
    }

    consola.info(`Added to SQS  Body:${JSON.stringify(messageBody)}`)
    const sqsData = await sendMessageToQueue(messageBody)
    influxdb(200, `send_to_affIds_to_sqs_success`)
    consola.info(`sqsData:${JSON.stringify(sqsData)}`)
    affiliateIdsUnique.clear()
  } catch (e) {
    influxdb(500, `send_to_affIds_to_sqs_error`)
    consola.error('sendToAffIdsToSqsError:', e)
  }

}

setInterval(sendToAffIdsToSqs, 7200000) // 7200000 ms -> 2h  28800000 ms -> 8h  300000 -> 5 MIN FOR TEST

export const aggregateDataProcessing = async (aggregationObject: object) => {

  const currentTime: number = Math.floor((new Date().getTime()) / 1000);

  const countRecords: number = Object.keys(aggregationObject).length
  if (countRecords >= 1) {
    consola.info(`Records:${countRecords} LIMIT_RECORDS:${LIMIT_RECORDS},  seconds have passed:${currentTime - getInitDateTime()!}, LIMIT_SECONDS:${LIMIT_SECONDS} ,  computerName:${computerName}`)
  }

  if (currentTime - getInitDateTime()! >= LIMIT_SECONDS
    && countRecords >= 1
  ) {
    consola.info(`ComputerName:${computerName}, pass ${LIMIT_SECONDS} seconds with records count:${countRecords}, process at event we have only one records`)
  }

  if (countRecords >= LIMIT_RECORDS
    || (
      countRecords >= 1
      && currentTime - getInitDateTime()! >= LIMIT_SECONDS
    )
  ) {
    try {
      let lids: Array<string> = []
      let records = ""
      for (const [key, value] of Object.entries(aggregationObject)) {
        let buffer = JSON.parse(Base64.decode(key))
        buffer.click = value.count;
        let timeCurrent: number = new Date().getTime()
        affiliateIdsUnique.add(buffer.affiliate_id)
        lids.push(buffer.lid)
        buffer.date_added = Math.floor(timeCurrent / 1000)
        records += JSON.stringify(buffer) + "\n";
      }
      let recordsReady = records.slice(0, -1)
      consola.info(`ComputerName:${computerName}. Lids count: { ${lids.length} }. Lids list:${lids}`)
      // influxdb(200, `lids_pool_${computerName}_count_${lids.length}`)
      // @ts-ignore
      Object.keys(aggregationObject).forEach(k => delete aggregationObject[k])
      setInitDateTime(null)
      // @ts-ignore
      let filePath = generateFilePath(localPath) || ''
      let fileFolder = path.dirname(filePath);
      await createRecursiveFolder(fileFolder)
      await appendToLocalFile(filePath, recordsReady)
      await compressFile(filePath)
      await copyGz(filePath)
      await deleteFile(filePath)
      consola.success(`DONE FIRST STEP computerName:${computerName}, create local gz file:${filePath}`)
      setTimeout(fileGzProcessing, 2000)
    } catch (e) {
      influxdb(500, `aggregate_data_processing_error`)
      consola.error('error generate zip file:', e)
    }
  }
}

const fileGzProcessing = async () => {
  try {
    const localFolder: string = process.env.FOLDER_LOCAL + '_gz' || ''
    const files = await getLocalFiles(localFolder)
    // consola.info(`gz files:${JSON.stringify(files)}`)
    if (files.length === 0) {
      consola.info('no zip files at:', localFolder)
      return
    }
    await filesToS3(files)
    setTimeout(copyZipFromS3Redshift, 2000, files)
    // await copyZipFromS3Redshift(files)
  } catch (e) {
    influxdb(500, `file_gz_processing_error`)
    consola.error('fileGzProcessingError:', e)
  }
}