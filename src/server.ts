import { createServer } from 'http';
import 'dotenv/config';
import consola from 'consola';

import express, {
  Application, Request, Response,
} from 'express';
import os from 'os';
import { influxdb } from './metrics';

import { aggregateDataProcessing } from './aggregatorData';
import {
  deleteFolder, getHumanDateFormat, getInitDateTime, setInitDateTime,
} from './utils';
import { IFolder, unprocessedS3Files } from './S3Handle';

const app: Application = express();
const httpServer = createServer(app);

const host: string = process.env.HOST || '';
const port: number = Number(process.env.PORT || '3001');

const computerName = os.hostname();

const localPath: string = `${process.cwd()}/${process.env.FOLDER_LOCAL}` || '';

app.get('/health', (req: Request, res: Response) => {
  res.json('Ok');
});

// https://aggregator.aezai.com/reUploadToRedshift
app.get('/reUploadToRedshift', (req: Request, res: Response) => {
  setTimeout(unprocessedS3Files, 2000, IFolder.UNPROCESSED);
  res.json({
    success: true,
    info: `added to queue  running after 2 seconds folder:{ ${IFolder.UNPROCESSED} }`,
  });
});

// https://aggregator.aezai.com/reUploadToRedshiftFailed
app.get('/reUploadToRedshiftFailed', (req: Request, res: Response) => {
  setTimeout(unprocessedS3Files, 2000, IFolder.FAILED);
  res.json({
    success: true,
    info: `added to queue running after 2 seconds folder:{ ${IFolder.FAILED} } `,
  });
});

app.use(express.json());

const aggregationObject: { [index: string]: any } = {};

app.post('/offer', async (req: Request, res: Response) => {
  try {
    const { key } = req.body;
    const { time } = req.body;

    influxdb(200, 'offer_get_click');

    if (!getInitDateTime()) {
      const currentTime: number = Math.floor((new Date().getTime()) / 1000);
      const currentDateHuman = new Date(currentTime * 1000);
      consola.info(`\nSetup setInitDateTime:${getHumanDateFormat(currentDateHuman)}, computerName:{ ${computerName} }`);
      setInitDateTime(currentTime);
    }

    if (key in aggregationObject) {
      aggregationObject[key].count += 1;
    } else {
      aggregationObject[key] = { time, count: 1 };
    }
    const countOfRecords = Object.keys(aggregationObject).length;
    const response = {
      time,
      countOfRecords,
    };

    res.status(200).json(response);
  } catch (e) {
    consola.error(e);
    res.json({ err: e });
  }
});

setInterval(aggregateDataProcessing, 9000, aggregationObject);

setInterval(deleteFolder, 36000000, localPath); // 36000000 ms -> 10h
setInterval(deleteFolder, 36000000, `${localPath}_gz`); // 36000000 ms ->  10h

setInterval(unprocessedS3Files, 32400000, IFolder.FAILED); // 32400000 ms ->  9h
setInterval(unprocessedS3Files, 28800000, IFolder.UNPROCESSED); // 28800000 ms ->  8h

httpServer.listen(port, host, (): void => {
  consola.success(`Server is running on http://${host}:${port} NODE_ENV:${process.env.NODE_ENV} Using node - { ${process.version} }`);
  consola.info(`S3_BUCKET_NAME:${process.env.S3_BUCKET_NAME}, AWS_ACCESS_KEY_ID:${process.env.AWS_ACCESS_KEY_ID}`);
});
