import {pool} from "../redshift";
import consola from "consola";

export const copyS3ToRedshift = async (file:string) => {
  let client = await pool.connect()

  let destPath = 'unprocessed/co-offers/' + file.substr(file.indexOf('unprocessed_json_gz') + 20, file.length)
  // consola.info('CopyS3ToRedshift file:', destPath)

  let awsKey = process.env.AWS_ACCESS_KEY_ID
  let awsSecretKey = process.env.AWS_SECRET_ACCESS_KEY
  let dbRedshift = `${process.env.REDSHIFT_SCHEMA}.${process.env.REDSHIFT_TABLE}`
  let bucket = process.env.S3_BUCKET_NAME
  consola.info('awsKey:',awsKey)
  consola.info('dbRedshift:',dbRedshift)
  consola.info('bucket:',bucket)
  consola.info('destPath:',destPath)
  // let file = '/unprocessed/co-offers/2021-09-24/13/20210924132442-292-523.json.gz'
  let queryCopy = `COPY ${dbRedshift} FROM 's3://${bucket}/${destPath}' CREDENTIALS 'aws_access_key_id=${awsKey};aws_secret_access_key=${awsSecretKey}' format as json 'auto' gzip MAXERROR 5 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS`;
  consola.info('queryCopy:', queryCopy)
  try{
    await client.query(queryCopy)
    consola.info(`File ${destPath} added to redshift successfully`)
    client.release()
    return true
  } catch (e) {
    consola.error('copyS3ToRedshiftError:',e)

  }
}