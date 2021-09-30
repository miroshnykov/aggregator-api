import fs from "fs";
import consola from "consola";
import * as moment from "moment-timezone";
import NodeDir from "node-dir";

export const generateFilePath = (localPath: string) => {
  try {
    const time = new Date();
    let getFromUnix = moment.utc(time)
    let pathPrefix = `${localPath}${getFromUnix.format('/YYYY-MM-DD/HH/')}`
    let randomDigits = Math.floor(Math.random() * (999 - 100 + 1) + 100)
    let fileName = `${getFromUnix.format('YYYYMMDDHHmmss')}-${time.getMilliseconds()}-${randomDigits}.json`
    return `${pathPrefix}${fileName}`
  } catch (e) {
    consola.error('generateFilePathError:', e)
  }
};

export const createRecursiveFolder = (fileFolder: string) => {
  return new Promise<boolean>((resolve, reject) => {
    fs.mkdir(fileFolder, {recursive: true}, function (err: any) {
      if (err) {
        reject(err);
      }
      resolve(true);
    })
  })
};

export const appendToLocalFile = (filePath: string, data: any) => {
  return new Promise((resolve, reject) => {
    // @ts-ignore
    fs.appendFileSync(filePath, data, (err: any) => {
      if (err) {
        reject(err);
      }
    });
    resolve(filePath)
  })
};

export const deleteFile = (filePath: string) => {
  return new Promise<string>((resolve, reject) => {
    fs.unlink(filePath, (err) => {
      if (err) {
        consola.error(`deleteFile :`, err)
        reject(filePath)
      }
    });
    resolve(filePath)
  })
};

export const getLocalFiles = (localFolder: string): Promise<string[]> => {
  return new Promise<string[]>((resolve, reject) => {
    NodeDir.files(localFolder, (err: any, files: string[]) => {
      if (err) {
        return reject(err);
      }
      files = files.filter((file) => (file.includes('.gz')))
      files.sort();
      return resolve(files);
    });
  })
};

export const deleteFolder = (dirPath: string): Promise<string> => {
  return new Promise<string>((resolve, reject) => {
    fs.rm(dirPath, { recursive: true }, (err:any) => {
      if (err) {
        consola.error(`deleteFolderError:${dirPath}`,err)
        reject(dirPath)
      }
      consola.info(`${dirPath} is deleted!`);
    });

    resolve(dirPath)
  })
};