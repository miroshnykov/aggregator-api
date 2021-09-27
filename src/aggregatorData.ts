import Base64 from "js-base64";
import {appendToLocalFile, createRecursiveFolder, deleteFile, generateFilePath} from "./utils";
import path from "path";
import {compressFile, copyGz} from "./zip";
import consola from "consola";
import {copyS3Files, copyZipToS3} from "./crons/copyZiptoS3";

const localPath: string = `${process.cwd()}/${process.env.FOLDER_LOCAL}` || ''
consola.info(`FOLDER_LOCAL:${localPath}`)

export const aggregateDataProcessing = async (aggregationObject: object) => {

  // consola.info(`count:${Object.keys(aggregationObject).length}`)
  if (Object.keys(aggregationObject).length >= 2) {
    try {
      let records = ""
      for (const [key, value] of Object.entries(aggregationObject)) {
        let buffer = JSON.parse(Base64.decode(key))
        buffer.click = value.count;
        let timeCurrent: number = new Date().getTime()
        buffer.date_added = Math.floor(timeCurrent / 1000)
        records += JSON.stringify(buffer) + "\n";
      }
      let recordsReady = records.slice(0, -1)
      // @ts-ignore
      Object.keys(aggregationObject).forEach(k => delete aggregationObject[k])
      // @ts-ignore
      let filePath = generateFilePath(localPath) || ''
      let fileFolder = path.dirname(filePath);
      // consola.info('filePath:', filePath)
      // consola.info('fileFolder:', fileFolder)
      await createRecursiveFolder(fileFolder)
      await appendToLocalFile(filePath, recordsReady)
      await compressFile(filePath)
      await copyGz(filePath)
      await deleteFile(filePath)
      consola.info(` *** DONE FIRST STEP CREATE LOCAL ZIP *** FILE:${filePath}`)

      setTimeout(copyZipToS3, 2000)
    } catch (e) {
      consola.error('error generate zip file:', e)
    }

  }

}