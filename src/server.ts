import {createServer} from "http";
import 'dotenv/config';
import consola from "consola";

import express, {Application, Request, Response, NextFunction} from 'express'
import Base64 from "js-base64"
import path from "path";
import {influxdb} from "./metrics";

const app: Application = express();
const httpServer = createServer(app);

const host: string = process.env.HOST || ''
const port: number = Number(process.env.PORT || '3001')

import {aggregateDataProcessing} from "./aggregatorData";
import {redshiftClient, pool} from "./redshift";
import {deleteFolder, getCreateAggrObjectTime, setCreateAggrObjectTime} from "./utils";
import {IFolder, unprocessedS3Files} from "./S3Handle";

const localPath: string = `${process.cwd()}/${process.env.FOLDER_LOCAL}` || ''

app.get('/health', (req: Request, res: Response) => {
  res.json('Ok')
})

app.get('/reUploadToRedshift', (req: Request, res: Response) => {
  setTimeout(unprocessedS3Files, 2000, IFolder.UNPROCESSED)
  res.json({
    success: true,
    info: `added to sqs`
  })
})


app.use(express.json())

const aggregationObject: { [index: string]: any } = {}

app.post('/offer', async (req: Request, res: Response) => {
  try {
    let key: string = req.body.key
    let time: string = req.body.time

    influxdb(200, `offer_get_click`)

    if (!getCreateAggrObjectTime()) {
      consola.info('Setup setCreateAggrObjectTime')
      setCreateAggrObjectTime(Math.floor((new Date().getTime()) / 1000))
    }

    if (key in aggregationObject) {
      aggregationObject[key]['count'] += 1
    } else {
      aggregationObject[key] = {"time": time, "count": 1}
    }
    const countOfRecords = Object.keys(aggregationObject).length
    let response = {
      time,
      countOfRecords
    }

    res.status(200).json(response)
  } catch (e) {
    consola.error(e)
    res.json({err: e})
  }

})

setInterval(aggregateDataProcessing, 9000, aggregationObject)

setInterval(deleteFolder, 36000000, localPath) // 36000000 ms -> 10h
setInterval(deleteFolder, 36000000, localPath + '_gz') // 36000000 ms ->  10h

httpServer.listen(port, host, (): void => {
  consola.success(`Server is running on http://${host}:${port} NODE_ENV:${process.env.NODE_ENV}`)
  consola.info(`S3_BUCKET_NAME:${process.env.S3_BUCKET_NAME}, AWS_ACCESS_KEY_ID:${process.env.AWS_ACCESS_KEY_ID}`)
});