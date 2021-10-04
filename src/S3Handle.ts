import fs from "fs";
import AWS from 'aws-sdk'
import consola from "consola";
import {ManagedUpload} from "aws-sdk/lib/s3/managed_upload";
import SendData = ManagedUpload.SendData;
import * as dotenv from "dotenv";

import {pool} from "./redshift";

dotenv.config();

import {deleteFile, getLocalFiles} from "./utils";

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

export const filesToS3 = async (files: string[]) => {

  try {
    for (const file of files) {
      // consola.info('file:', file)
      let successUpload = await uploadFileToS3Bucket(file)
      if (successUpload) {
        // consola.info('deleteFile:', file)
        await deleteFile(file)
      }
    }
    consola.success(`DONE SECOND STEP send gz to s3:${JSON.stringify(files)}`)

  } catch (e) {
    consola.error('s3Handle:', e)
  }

}

export const copyZipFromS3Redshift = async (files: string[]) => {

  try {
    for (const file of files) {
      let copyS3ToRedshiftResponse = await copyS3ToRedshift(file)
      if (copyS3ToRedshiftResponse) {
        await copyS3Files(file, Folder.PROCESSED)
      } else {
        await copyS3Files(file, Folder.FAILED)
      }
      await deleteS3Files(file)
    }
    consola.success(`DONE THIRD STEP copy file to redshift:${JSON.stringify(files)}\n`)
  } catch (e) {
    consola.error('copyZipFromS3RedshiftError:', e)
  }

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

  let destPath = 'unprocessed/co-offers/' + file.substr(file.indexOf('unprocessed_json_gz') + 20, file.length)
  let destKey = '/co-offers/' + destPath.substr(destPath.indexOf('unprocessed_json_gz') + 23, destPath.length)
  return new Promise<boolean>((resolve, reject) => {
    let bucket = process.env.S3_BUCKET_NAME || ''
    const params = {
      Bucket: bucket,
      CopySource: bucket + '/' + destPath,
      Key: folder + destKey,
    };
    s3.copyObject(params, (err, data) => {
      if (err) {
        console.log(err);
      } else {
        resolve(true)
      }
    })
  })
}

export const deleteS3Files = async (file: string) => {

  let destPath = 'unprocessed/co-offers/' + file.substr(file.indexOf('unprocessed_json_gz') + 20, file.length)

  return new Promise<boolean>((resolve, reject) => {
    let bucket = process.env.S3_BUCKET_NAME || ''
    const params = {
      Bucket: bucket,
      Key: destPath,
    };
    s3.deleteObject(params, (err, data) => {
      if (err) {
        consola.error(err);
      } else {
        // consola.success('deleted file destPath:',destPath)
        resolve(true)
      }
    })
  })
}

export const copyS3ToRedshift = async (file:string) => {
  let client = await pool.connect()
  let destPath = 'unprocessed/co-offers/' + file.substr(file.indexOf('unprocessed_json_gz') + 20, file.length)

  let awsKey = process.env.AWS_ACCESS_KEY_ID
  let awsSecretKey = process.env.AWS_SECRET_ACCESS_KEY
  let dbRedshift = `${process.env.REDSHIFT_SCHEMA}.${process.env.REDSHIFT_TABLE}`
  let bucket = process.env.S3_BUCKET_NAME
  let queryCopy = `COPY ${dbRedshift} FROM 's3://${bucket}/${destPath}' CREDENTIALS 'aws_access_key_id=${awsKey};aws_secret_access_key=${awsSecretKey}' format as json 'auto' gzip MAXERROR 5 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS`;
  // consola.info('queryCopy:', queryCopy)
  try{
    await client.query(queryCopy)
    consola.info(`File ${destPath} added to redshift successfully`)
    client.release()
    return true
  } catch (e) {
    consola.error('copyS3ToRedshiftError:',e)
  }
}
