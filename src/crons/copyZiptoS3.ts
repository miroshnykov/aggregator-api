import fs from "fs";
import AWS from 'aws-sdk'
import consola from "consola";
import {ManagedUpload} from "aws-sdk/lib/s3/managed_upload";
import SendData = ManagedUpload.SendData;
import * as dotenv from "dotenv";

dotenv.config();

import {deleteFile, getLocalFiles} from "../utils";
import {copyS3ToRedshift} from "./copyS3ToRedshift";

enum Folder {
  PROCESSED = 'processed',
  FAILED = 'failed'
}

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION
});
const s3 = new AWS.S3();

export const copyZipToS3 = async () => {
  const localFolder: string = process.env.FOLDER_LOCAL + '_gz' || ''
  const files = await getLocalFiles(localFolder)
  consola.info('Zip files:', files)
  if (files.length === 0) {
    consola.info('no zip files at:', localFolder)
    return
  }
  for (const file of files) {
    // consola.info('file:', file)
    let successUpload = await uploadFileToS3Bucket(file)
    if (successUpload) {
      consola.info('deleteFile:', file)
      await deleteFile(file)
    }
  }
  consola.info(' *** DONE SECOND SEND ZIP TO S# STEP *** ')

  for (const file of files) {
    let copyS3ToRedshiftResponse = await copyS3ToRedshift(file)
    if (copyS3ToRedshiftResponse) {
      await copyS3Files(file, Folder.PROCESSED)
    } else {
      await copyS3Files(file, Folder.FAILED)
    }

  }
  consola.info(' *** DONE THE THIRD COPY FILES TO REDSHIFT STEP *** ')
}

const uploadFileToS3Bucket = async (file: string) => {
  try {
    return new Promise<boolean>((resolve, reject) => {
      fs.readFile(file, (err, data) => {
        let destPath = 'unprocessed/co-offers/' + file.substr(file.indexOf('unprocessed_json_gz') + 20, file.length)
        // consola.info('destPath:', destPath)
        if (err) throw err;
        let s3Key: string = destPath || ''
        let s3BucketName: string = process.env.S3_BUCKET_NAME || ''

        const params = {
          Bucket: s3BucketName,
          Key: s3Key,
          Body: data
        };

        s3.upload(params, (err: Error, data: SendData) => {
          if (err) {
            consola.error(err);
            reject()
          }
          consola.info(`File uploaded successfully at S3 ${data.Location}`);
          resolve(true)
        });
      });

    })

  } catch (error) {
    console.error('s3 upload error:', error)
  } finally {

  }

}

export const copyS3Files = async (file: string, folder: Folder) => {

  consola.info('copyS3Files to folder:', folder)
  let destPath = 'unprocessed/co-offers/' + file.substr(file.indexOf('unprocessed_json_gz') + 20, file.length)
  consola.info('copyS3Files to file destPath:', destPath)
  let destKey = '/co-offers/' + destPath.substr(destPath.indexOf('unprocessed_json_gz') + 20, destPath.length)
  return new Promise<boolean>((resolve, reject) => {
    let bucket = 'co-aggregator-staging'
    const params = {
      Bucket: bucket,
      // CopySource: bucket + '/unprocessed/co-offers/2021-09-26/01/20210926014439-795-519.json.gz',
      CopySource: bucket + '/' + destPath,
      Key: folder + '/co-offers/2021-09-26/01/20210926014439-795-519.json.gz',
    };
    s3.copyObject(params, (err, data) => {
      if (err) {
        console.log(err);
      } else {
        // console.log(data);
        resolve(true)
      }

    })
  })

}




