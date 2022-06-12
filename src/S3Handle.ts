import fs from 'node:fs';
import AWS from 'aws-sdk';
import consola from 'consola';
import { ManagedUpload } from 'aws-sdk/lib/s3/managed_upload';
import * as dotenv from 'dotenv';
import os from 'node:os';
import SendData = ManagedUpload.SendData;

import { pool } from './redshift';

import { deleteFile } from './utils';
import { influxdb } from './metrics';
import { convertHrtime } from './convertHrtime';
import { DAYS_PERIOD } from './constants/constants';

const computerName = os.hostname();

dotenv.config();

export enum IFolder {
  PROCESSED = 'processed',
  FAILED = 'failed',
  UNPROCESSED = 'unprocessed',
}

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
});
const s3 = new AWS.S3();

// consola.info('AWS_ACCESS_KEY_ID:', process.env.AWS_ACCESS_KEY_ID);
// consola.info('AWS_SECRET_ACCESS_KEY:', process.env.AWS_SECRET_ACCESS_KEY);

// eslint-disable-next-line consistent-return
const uploadFileToS3Bucket = async (file: string) => {
  try {
    return await new Promise<boolean>((resolve, reject) => {
      fs.readFile(file, (err, data) => {
        const destPath = `unprocessed/${computerName}/co-offers/${file.substr(file.indexOf('unprocessed_json_gz') + 20, file.length)}`;
        if (err) throw err;
        const s3Key: string = destPath || '';
        const s3BucketName: string = process.env.S3_BUCKET_NAME || '';

        const params = {
          Bucket: s3BucketName,
          Key: s3Key,
          Body: data,
        };

        // eslint-disable-next-line @typescript-eslint/no-shadow
        s3.upload(params, (e: Error, data: SendData) => {
          if (e) {
            consola.error(e);
            reject();
          }
          consola.info(`File uploaded successfully at S3 ${data.Location}`);
          influxdb(200, `copy_gz_file_to_s3_success_${computerName}`);
          resolve(true);
        });
      });
    });
  } catch (error) {
    influxdb(500, `copy_gz_file_to_s3_error_${computerName}`);
    consola.error('s3 upload error:', error);
  }
};

export const filesToS3 = async (files: string[]): Promise<void> => {
  try {
    await Promise.all(files.map(async (file: string) => {
      const startTime: bigint = process.hrtime.bigint();
      const successUpload = await uploadFileToS3Bucket(file);

      if (successUpload) {
        const endTime: bigint = process.hrtime.bigint();
        const diffTime: bigint = endTime - startTime;
        consola.success(`DONE SECOND STEP { filesToS3 } time { ${convertHrtime(diffTime)} } ms, send gz to s3:${JSON.stringify(files)} computerName:{ ${computerName} }`);
        await deleteFile(file);
      }
    }));
  } catch (e) {
    influxdb(500, 'files_to_s3_error');
    consola.error('s3Handle:', e);
  }
};

// eslint-disable-next-line consistent-return
export const copyS3ToRedshift = async (destPath: string): Promise<boolean> => {
  const client = await pool.connect();

  const awsKey = process.env.AWS_ACCESS_KEY_ID;
  const awsSecretKey = process.env.AWS_SECRET_ACCESS_KEY;
  const dbRedshift = `${process.env.REDSHIFT_SCHEMA}.${process.env.REDSHIFT_TABLE}`;
  const bucket = process.env.S3_BUCKET_NAME;
  const queryCopy = `COPY ${dbRedshift} FROM 's3://${bucket}/${destPath}' CREDENTIALS 'aws_access_key_id=${awsKey};aws_secret_access_key=${awsSecretKey}' format as json 'auto' gzip MAXERROR 5 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS`;
  // consola.info(`REDSHIFT_HOST: { ${process.env.REDSHIFT_HOST} } REDSHIFT_USER: { ${process.env.REDSHIFT_USER} }  REDSHIFT_PORT: { ${process.env.REDSHIFT_PORT} } REDSHIFT_TABLE: { ${process.env.REDSHIFT_TABLE} } REDSHIFT_DATABASE: { ${process.env.REDSHIFT_DATABASE} } REDSHIFT_SCHEMA: { ${process.env.REDSHIFT_SCHEMA} }`);
  // consola.info('queryCopy:', queryCopy);
  try {
    await client.query(queryCopy);
    consola.info(`File ${destPath} added to redshift successfully`);
    influxdb(200, `copy_file_s3_to_redshift_success_${computerName}`);
    client.release();
    return true;
  } catch (e) {
    influxdb(500, `copy_file_s3_to_redshift_error_${computerName}`);
    consola.error('copyS3ToRedshiftError:', e);
    return false;
  }
};

export const copyS3Files = async (file: string, folder: IFolder) => {
  const path = file.substr(file.indexOf('unprocessed_json_gz') + 20, file.length);
  const destPath = `unprocessed/${computerName}/co-offers/${path}`;
  const destKey = `co-offers/${path}`;

  const bucket = process.env.S3_BUCKET_NAME || '';
  // consola.info(`CopyS3Files CopySource:${bucket}/${destPath}, Key:${folder}/${destKey}`);
  return new Promise<boolean>((resolve) => {
    const params = {
      Bucket: bucket,
      CopySource: `${bucket}/${destPath}`,
      Key: `${folder}/${destKey}`,
    };
    s3.copyObject(params, (err) => {
      if (err) {
        influxdb(500, 'copy_s3_files_error');
        consola.error(err);
      } else {
        resolve(true);
      }
    });
  });
};

export const deleteS3Files = async (destPath: string) => new Promise<boolean>((resolve) => {
  const bucket = process.env.S3_BUCKET_NAME || '';
  const params = {
    Bucket: bucket,
    Key: destPath,
  };
  s3.deleteObject(params, (err) => {
    if (err) {
      influxdb(500, 'delete_s3_file_error');
      consola.error(err);
    } else {
      // consola.success('deleted file destPath:',destPath)
      resolve(true);
    }
  });
});

export const copyGzFromS3Redshift = async (files: string[]) => {
  try {
    const startTime: bigint = process.hrtime.bigint();
    const filesDestPath: string[] = [];
    await Promise.all(files.map(async (file: string) => {
      const destPath = `unprocessed/${computerName}/co-offers/${file.substr(file.indexOf('unprocessed_json_gz') + 20, file.length)}`;
      filesDestPath.push(destPath!);
      const copyS3ToRedshiftResponse: boolean = await copyS3ToRedshift(destPath);
      await copyS3Files(file, copyS3ToRedshiftResponse ? IFolder.PROCESSED : IFolder.FAILED);
      await deleteS3Files(destPath);
      const endTime: bigint = process.hrtime.bigint();
      const diffTime: bigint = endTime - startTime;
      consola.success(`DONE THIRD STEP status { ${copyS3ToRedshiftResponse ? IFolder.PROCESSED : IFolder.FAILED} } { copyGzFromS3Redshift } time: { ${convertHrtime(diffTime)} } ms, copy file to s3 folder-${copyS3ToRedshiftResponse ? IFolder.PROCESSED : IFolder.FAILED}, deleted files:${JSON.stringify(filesDestPath)} computerName:{ ${computerName} }\n`);
    }));
  } catch (e) {
    influxdb(500, 'copy_zip_from_s3_redshift_error');
    consola.error('copyGzFromS3RedshiftError:', e);
  }
};

export const unprocessedS3Files = async (folder: IFolder) => {
  try {
    const bucket = process.env.S3_BUCKET_NAME || '';
    const params = {
      Bucket: bucket,
      Prefix: `${folder}/`,
    };
    const filesPath: string[] = [];
    const s3Objects = await s3.listObjects(params).promise();
    for (const content of s3Objects?.Contents!) {
      filesPath.push(content.Key!);
    }

    if (filesPath.length === 0) {
      consola.warn(`Cron ** unprocessedS3Files **  There is no files on s3 folder:{ ${folder} }`);
      return;
    }
    await Promise.all(filesPath.map(async (filePath: string) => {
      const copyS3ToRedshiftResponse: boolean = await copyS3ToRedshift(filePath);
      if (copyS3ToRedshiftResponse) {
        await deleteS3Files(filePath);
        consola.warn(` ** unprocessedS3Files ** folder: { ${folder} }  in bucket: { ${bucket} } reSend to redshift files:`, filePath);
        influxdb(200, `unprocessed_s3_files_${folder}_send_success`);
      }
    }));
  } catch (e) {
    consola.error(e);
    influxdb(500, `unprocessed_s3_files_error_${folder}`);
  }
};

const toTimeStamp = (strDate: any) => Date.parse(strDate);
const getHumanDateFormat = (date: any) => date.toISOString().replace(/T/, ' ').replace(/\..+/, '');

export const processedS3FilesCleanUp = async (folder: IFolder) => {
  try {
    const bucket = process.env.S3_BUCKET_NAME || '';
    const params = {
      Bucket: bucket,
      Prefix: `${folder}/`,
    };
    const filesPath: string[] = [];
    const s3Objects = await s3.listObjects(params).promise();

    const date = new Date();
    date.setDate(date.getDate() - DAYS_PERIOD);
    consola.log(`S3 files delete that was created before ${getHumanDateFormat(date)}`);
    let recordTotalCount: number = 0;
    let deleteRecordCount: number = 0;

    for (const content of s3Objects?.Contents!) {
      recordTotalCount++;
      if (date.getTime() > toTimeStamp(content.LastModified) && recordTotalCount < 500) {
        filesPath.push(content.Key!);
      }
    }
    for (const filePath of filesPath) {
      // eslint-disable-next-line no-await-in-loop
      const resDel = await deleteS3Files(filePath);
      if (resDel) {
        deleteRecordCount++;
        consola.success(` ** delete file { ${filePath} } folder: { ${folder} }  in bucket: { ${bucket} }`);
        influxdb(200, 'processed_s3_old_files_deleted');
      }
    }
    consola.info(`Total Records { ${recordTotalCount} }, ready to delete Records { ${filesPath.length} } delete Records { ${deleteRecordCount} } `);
  } catch (e) {
    consola.error(e);
    influxdb(500, 'processed_s3_files_clean_up_error');
  }
};
