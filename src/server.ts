import {createServer} from "http";
import 'dotenv/config';
import consola from "consola";

import express, {Application, Request, Response, NextFunction} from 'express'
import Base64 from "js-base64"
import path from "path";

const app: Application = express();
const httpServer = createServer(app);

const host: string = process.env.HOST || ''
const port: number = Number(process.env.PORT || '3001')

import {aggregateDataProcessing} from "./aggregatorData";
import {redshiftClient, pool} from "./redshift";

app.get('/health', (req: Request, res: Response) => {
  res.json('Ok')
})
app.use(express.json())

const aggregationObject: { [index: string]: any } = {}

// http://localhost:9002/offer
app.get('/sql', async (req: Request, res: Response) => {
//   let client = await pool.connect()
//
//
//   let awsKey = process.env.AWS_ACCESS_KEY_ID
//   let awsSecretKey = process.env.AWS_SECRET_ACCESS_KEY
//   let dbRedshift = 'advertiser_events.traffic'
//   let bucket = process.env.S3_BUCKET_NAME
//   let file = 'unprocessed/co-offers/2021-09-24/13/20210924132442-292-523.json.gz'
//   let queryCopy = `COPY ${dbRedshift} FROM 's3://${bucket}${file}' CREDENTIALS 'aws_access_key_id=${awsKey};aws_secret_access_key=${awsSecretKey}' format as json 'auto' gzip MAXERROR 5 ACCEPTINVCHARS TRUNCATECOLUMNS TRIMBLANKS`;
// // const res1 = await client.query('SELECT NOW()')
//   try{
//     await client.query(queryCopy)
//     console.log(`File ${file} added to redshift successfully`)
//     let obj = {
//       file: file
//     }
//     client.release()
//     res.status(200).json(obj)
//   } catch (e) {
//     consola.error(e)
//     res.json({err: e})
//   }


  //
  // client.query('SELECT NOW()', (err:any, res:any) => {
  //   console.log(err, res)
  //   client.end()
  // })

})

app.post('/offer', async (req: Request, res: Response) => {
  try {
    let key: string = req.body.key
    let time: string = req.body.time

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

httpServer.listen(port, host, (): void => {
  consola.success(`Server is running on http://${host}:${port} NODE_ENV:${process.env.NODE_ENV}`)
  consola.info(`S3_BUCKET_NAME:${process.env.S3_BUCKET_NAME}, AWS_ACCESS_KEY_ID:${process.env.AWS_ACCESS_KEY_ID}`)
});