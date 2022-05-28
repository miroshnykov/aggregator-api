import fs from 'node:fs';
import zlib from 'node:zlib';
import path from 'node:path';
import consola from 'consola';
import { createRecursiveFolder, renameFile } from './utils';
import { influxdb } from './metrics';

export const compressFile = (fileName: string) => new Promise((resolve) => {
  const read = fs.createReadStream(fileName);
  const write = fs.createWriteStream(`${fileName}.gz`);
  const compress = zlib.createGzip();
  read.pipe(compress).pipe(write);
  // eslint-disable-next-line consistent-return
  compress.on('unpipe', (compression) => {
    // eslint-disable-next-line no-underscore-dangle
    if (compression._readableState.ended === true) {
      // console.log('Compression stream ended');
      // eslint-disable-next-line @typescript-eslint/no-shadow
      return new Promise((resolve) => {
        write.on('finish', () => {
          // console.log('Compression fully finished');
          resolve(write);
        });
      }).then(() => {
        // consola.info(`Compression fully finished. FileName:${fileName}`)
        resolve(fileName);
      }).catch((err) => {
        consola.error(`zip error fileName:${fileName}`, err);
        influxdb(500, 'compress_zip_file_error');
      });
    }
  });
  compress.on('errors', (err) => {
    consola.error(`Zip compress error: fileName:${fileName}`, err);
  });
  write.on('error', (err) => {
    consola.error(`Zip write error: fileName:${fileName}`, err);
  });
}).catch((err) => {
  consola.error(`compressFileZlibError fileName:${fileName}`, err);
});

export const copyGz = (filePath: string) => {
  const gzPath: string = `${filePath}.gz`;
  let newPath: string = filePath.replace('unprocessed_json', 'unprocessed_json_gz');
  const newFileFolder = path.dirname(newPath);
  newPath = `${newPath}.gz`;
  return createRecursiveFolder(newFileFolder)
    .then(() => {
      renameFile(gzPath, newPath).then(() => filePath);
    });
};
