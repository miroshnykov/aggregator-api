import fs from "fs";
import zlib from "zlib";
import path from "path";
import {createRecursiveFolder} from "./utils";
import consola from "consola";

export const compressFile = (fileName: string) => {
  return new Promise((resolve) => {
    let read = fs.createReadStream(fileName)
    let write = fs.createWriteStream(fileName + '.gz')
    let compress = zlib.createGzip()
    read.pipe(compress).pipe(write)
    compress.on('unpipe', (compression) => {
      if (compression._readableState.ended === true) {
        // console.log('Compression stream ended');
        return new Promise((resolve) => {
          write.on('finish', () => {
            // console.log('Compression fully finished');
            resolve(write);
          })
        }).then(() => {
          consola.info(`Compression fully finished. FileName:${fileName}`)
          resolve(fileName)
        }).catch((err) => {
          console.error(`zip error fileName:${fileName}`, err)
        })
      }
    })
    compress.on('errors', (err) => {
      consola.error(`Zip compress error: fileName:${fileName}`, err)
    })
    write.on('error', (err) => {
      consola.error(`Zip write error: fileName:${fileName}`, err)
    })
  }).catch((err) => {
    console.log(`compressFileZlibError fileName:${fileName}`, err)
  })
}

export const copyGz = (filePath: string) => {
  let gzPath: string = `${filePath}.gz`;
  let newPath: string = filePath.replace("unprocessed_json", "unprocessed_json_gz");
  let newFileFolder = path.dirname(newPath);
  newPath = `${newPath}.gz`
  return createRecursiveFolder(newFileFolder)
    .then(() => {
      fs.renameSync(gzPath, newPath);
      return filePath;
    })
};