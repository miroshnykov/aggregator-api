import fs from 'node:fs';
import consola from 'consola';
import * as moment from 'moment-timezone';
import NodeDir from 'node-dir';
import { influxdb } from './metrics';

let initDateTime: number | null = null;

export const generateFilePath = (localPath: string) => {
  const time = new Date();
  const getFromUnix = moment.utc(time);
  const pathPrefix = `${localPath}${getFromUnix.format('/YYYY-MM-DD/HH/')}`;
  const randomDigits = Math.floor(Math.random() * (999 - 100 + 1) + 100);
  const fileName = `${getFromUnix.format('YYYYMMDDHHmmss')}-${time.getMilliseconds()}-${randomDigits}.json`;
  return `${pathPrefix}${fileName}`;
};

export const createRecursiveFolder = (fileFolder: string) => new Promise<boolean>((resolve, reject) => {
  fs.mkdir(fileFolder, { recursive: true }, (err: any) => {
    if (err) {
      influxdb(500, 'create_recursive_folder_error');
      reject(err);
    }
    resolve(true);
  });
});

export const appendToLocalFile = (filePath: string, data: any) => new Promise((resolve, reject) => {
  fs.appendFile(filePath, data, (err: any) => {
    if (!err) {
      influxdb(500, 'append_to_local_file_error');
      consola.error(`appendToLocalFileError ${filePath}:`, err);
      reject(err);
    }
  });
  resolve(filePath);
});

export const deleteFile = (filePath: string) => new Promise<string>((resolve, reject) => {
  fs.unlink(filePath, (err) => {
    if (err) {
      // consola.error(`deleteFile :`, err)
      influxdb(500, 'delete_file_error');
      reject(filePath);
    }
  });
  resolve(filePath);
});

export const getLocalFiles = (localFolder: string): Promise<string[]> => new Promise<string[]>((resolve, reject) => {
  NodeDir.files(localFolder, (err: any, files: string[]) => {
    if (err) {
      influxdb(500, 'get_local_folder_error');
      return reject(err);
    }
    // eslint-disable-next-line no-param-reassign
    files = files.filter((file) => (file.includes('.gz')));
    files.sort();
    return resolve(files);
  });
});

export const deleteFolder = (dirPath: string): Promise<string> => new Promise<string>((resolve, reject) => {
  fs.rm(dirPath, { recursive: true }, (err: any) => {
    if (err) {
      consola.error(`deleteFolderError:${dirPath}`, err);
      influxdb(500, 'delete_folder_error');
      reject(dirPath);
    }
    consola.info(`${dirPath} is deleted!`);
  });

  resolve(dirPath);
});
export const setInitDateTime = (dateTime: number | null): void => {
  initDateTime = dateTime;
};

export const getInitDateTime = (): number | null => (initDateTime);

const replace = (str: string): string => (
  str.replace(/T/, ' ').replace(/\..+/, '')
);

export const getHumanDateFormat = (date: Date): string => (replace(date.toISOString()));
