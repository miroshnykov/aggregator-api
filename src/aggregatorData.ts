import Base64 from "js-base64";
import {
  appendToLocalFile,
  createRecursiveFolder,
  deleteFile,
  generateFilePath,
  getLocalFiles,
  getCreateAggrObjectTime,
  setCreateAggrObjectTime
} from "./utils";
import path from "path";
import {compressFile, copyGz} from "./zip";
import consola from "consola";
import {copyS3Files, copyZipFromS3Redshift, filesToS3} from "./S3Handle";
import {createDeflateRaw} from "zlib";
import {sendMessageToQueue} from "./sqs"
import {influxdb} from "./metrics";
import os from "os"
const computerName = os.hostname()

const localPath: string = `${process.cwd()}/${process.env.FOLDER_LOCAL}` || ''
consola.info(`FOLDER_LOCAL:${localPath}`)

const affiliateIdsUnique = new Set();

const sendToAffIdsToSqs = async () => {
  try {
    let uniques = Array.from(affiliateIdsUnique)
    if (uniques.length === 0) return

    const messageBody = {
      body: JSON.stringify({
        type: 'traffic',
        affiliatesId: uniques,
        timestamp: Date.now()
      })
    }

    consola.info(`Added to SQS  Body:${JSON.stringify(messageBody)}`)
    let sqsData = await sendMessageToQueue(messageBody)
    influxdb(200, `send_to_affIds_to_sqs_success`)
    consola.info(`sqsData:${JSON.stringify(sqsData)}`)
    affiliateIdsUnique.clear()
  } catch (e) {
    influxdb(500, `send_to_affIds_to_sqs_error`)
    consola.error('')
  }

}

setInterval(sendToAffIdsToSqs, 7200000) // 7200000ms -> 2h  28800000 ms -> 8h  300000 -> 5 MIN FOR TEST

export const aggregateDataProcessing = async (aggregationObject: object) => {

  let currentTime = Math.floor((new Date().getTime()) / 1000);
  if (getCreateAggrObjectTime()) {
    consola.info(`Create Aggregate Object init computerName:${computerName}, second left:${currentTime - getCreateAggrObjectTime()}`)
  }
  if (Object.keys(aggregationObject).length >= 1) {
    consola.info(`Count clicks in pool:${Object.keys(aggregationObject).length} computerName:${computerName}`)
  }

  if (getCreateAggrObjectTime() && currentTime - getCreateAggrObjectTime() >= 60 && Object.keys(aggregationObject).length >= 1) { // 60 sec
    consola.info(`ComputerName:${computerName}, pass 60 sec with records count:${Object.keys(aggregationObject).length}, process at event we have only one records`)
  }

  if (Object.keys(aggregationObject).length >= 20
    || (Object.keys(aggregationObject).length >= 1 && getCreateAggrObjectTime() && currentTime - getCreateAggrObjectTime() >= 60) // 60 sec
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
      consola.info(`ComputerName:${computerName}. Lids count${lids.length}:${lids}`)
      // @ts-ignore
      Object.keys(aggregationObject).forEach(k => delete aggregationObject[k])
      setCreateAggrObjectTime(null)
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