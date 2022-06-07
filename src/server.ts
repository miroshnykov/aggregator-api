import { createServer } from 'node:http';
import 'dotenv/config';
import consola from 'consola';

import express, {
  Application, Request, Response,
} from 'express';
import os from 'node:os';
import md5 from 'md5';
import { influxdb } from './metrics';

import { aggregateDataProcessing } from './aggregatorData';
import {
  deleteFolder, getHumanDateFormat, getInitDateTime, setInitDateTime,
} from './utils';
import { IFolder, processedS3FilesCleanUp, unprocessedS3Files } from './S3Handle';
import { insertBonusLid, selectLid } from './redshift';
import { IBonusLidRes } from './Interfaces/traffic';
import { sendLidDynamoDb } from './dynamoDb';
import { ILid } from './Interfaces/lid';
import { IntervalTime } from './constants/intervalTime';

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
// https://aggregator.stage.aezai.com/reUploadToRedshift
app.get('/reUploadToRedshift', (req: Request, res: Response) => {
  try {
    if (!req.query.hash || req.query.hash !== process.env.GATEWAY_API_SECRET) {
      throw Error('broken key');
    }
    setTimeout(unprocessedS3Files, 2000, IFolder.UNPROCESSED);
    res.json({
      success: true,
      info: `added to queue  running after 2 seconds folder:{ ${IFolder.UNPROCESSED} }`,
    });
  } catch (e: any) {
    res.json({
      success: false,
      info: e.toString(),
    });
  }
});

// https://aggregator.aezai.com/reUploadToRedshiftFailed
app.get('/reUploadToRedshiftFailed', (req: Request, res: Response) => {
  try {
    if (!req.query.hash || req.query.hash !== process.env.GATEWAY_API_SECRET) {
      throw Error('broken key');
    }
    setTimeout(unprocessedS3Files, 2000, IFolder.FAILED);
    res.json({
      success: true,
      info: `added to queue running after 2 seconds folder:{ ${IFolder.FAILED} } `,
    });
  } catch (e: any) {
    res.json({
      success: false,
      info: e.toString(),
    });
  }
});

// http://localhost:9002/reSendLidToDynamoDb?lid=764a3590-3533-4c53-a88a-4bc9d23d6899&hash=
// https://aggregator.aezai.com/reSendLidToDynamoDb?lid=764a3590-3533-4c53-a88a-4bc9d23d6899&hash=SECRET

app.get('/reSendLidToDynamoDb', async (req: Request, res: Response) => {
  try {
    if (!req.query.hash || req.query.hash !== process.env.GATEWAY_API_SECRET) {
      throw Error('broken key');
    }
    const lid: string = String(req.query.lid!) || '';
    const hash: string = String(req.query.hash!) || '';
    consola.info(`lid ${lid} hash ${hash}`);

    const lidInfo: any = await selectLid(lid);
    if (!lidInfo) {
      throw Error(`lid:${lid} does not exists in redshift ${process.env.REDSHIFT_HOST}`);
    }

    // eslint-disable-next-line prefer-destructuring
    const convertToLidDynamoDb: ILid = {
      lid: lidInfo.lid,
      affiliateId: +lidInfo.affiliate_id! || 0,
      campaignId: +lidInfo.campaign_id! || 0,
      subCampaign: lidInfo.sub_campaign! || '',
      offerId: +lidInfo.offer_id! || 0,
      offerName: lidInfo.offer_name! || '',
      offerType: lidInfo.offer_type! || '',
      offerDescription: lidInfo.offer_description! || '',
      landingPageUrl: lidInfo.landing_page || '',
      landingPageId: +lidInfo.landing_page_id! || 0,
      payin: lidInfo.payin || 0,
      payout: lidInfo.payout || 0,
      country: lidInfo.geo || '',
      advertiserId: +lidInfo.advertiser_id! || 0,
      advertiserManagerId: +lidInfo.advertiser_manager_id! || 0,
      affiliateManagerId: +lidInfo.affiliate_manager_id! || 0,
      originAdvertiserId: +lidInfo.origin_advertiser_id! || 0,
      originConversionType: lidInfo.origin_conversion_type || '',
      verticalId: lidInfo.verticals || 0,
      verticalName: lidInfo.vertical_name || '',
      conversionType: lidInfo.conversion_type || '',
      platform: lidInfo.platform || '',
      deviceType: lidInfo.device! || '',
      os: lidInfo.os || '',
      isp: lidInfo.isp || '',
      referer: lidInfo.referer || '',
      adDomain: '',
      adPath: '',
      domain: '',
      advertiserName: '',
      region: '',
      IP: '',
      sflServer: '',
      userAgent: '',
      browser: '',
      browserEngine: '',
      browserVersion: '',
      payoutPercent: 0,
      isCpmOptionEnabled: 0,
      originPayIn: 0,
      originPayOut: 0,
      originAdvertiserName: '',
      originIsCpmOptionEnabled: 0,
      originOfferId: 0,
      originVerticalId: 0,
      originVerticalName: '',
      landingPageIdOrigin: 0,
      landingPageUrlOrigin: '',
      capOverrideOfferId: 0,
      offerIdRedirectExitTraffic: 0,
      redirectType: '',
      redirectReason: '',
      capsType: '',
      isUseDefaultOfferUrl: false,
      nestedExitOfferSteps: '',
      fingerPrintInfo: '',
      fingerPrintKey: '',
      isUniqueVisit: null,
      eventType: '',
      _messageType: '',
    };

    // res.json({
    //   success: true,
    //   lid,
    //   info: convertToLidDynamoDb,
    // });
    //
    // return;
    const response = await sendLidDynamoDb(convertToLidDynamoDb);
    if (!response) {
      throw Error(`lid:${lid} does not create in DynamoDb table ${process.env.AWS_DYNAMODB_TABLE_NAME}`);
    }

    res.json({
      success: true,
      lid,
      info: response,
    });
  } catch (e: any) {
    res.json({
      success: false,
      info: e.toString(),
    });
  }
});

// http://localhost:9002/processedS3FilesCleanUp?hash=dede
// https://aggregator.aezai.com/processedS3FilesCleanUp
app.get('/processedS3FilesCleanUp', (req: Request, res: Response) => {
  try {
    if (!req.query.hash || req.query.hash !== process.env.GATEWAY_API_SECRET) {
      throw Error('broken key');
    }
    setTimeout(processedS3FilesCleanUp, 2000, IFolder.PROCESSED);
    res.json({
      success: true,
      info: `added to queue processedS3FilesCleanUp running after 2 seconds folder:{ ${IFolder.PROCESSED} } `,
    });
  } catch (e: any) {
    res.json({
      success: false,
      info: e.toString(),
    });
  }
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

app.post('/lidBonus', async (req: Request, res: Response) => {
  const { stats } = req.body;
  const { hash } = req.body;
  const { timestamp } = req.body;
  const response: IBonusLidRes = {
    timestamp,
    success: false,
  };
  try {
    const secret = process.env.GATEWAY_API_SECRET;
    const checkHash = md5(`${timestamp}|${secret}`);

    if (checkHash !== hash) {
      response.errors = 'Broken hash';
      res.status(200).json(response);
      return;
    }

    const insertBonusLidRes: boolean = await insertBonusLid(stats);
    if (insertBonusLidRes) {
      response.success = true;
    }
    res.status(200).json(response);
  } catch (e) {
    consola.error(e);
    response.errors = JSON.stringify(e);
    res.status(500).json(response);
  }
});

setInterval(aggregateDataProcessing, IntervalTime.DATA_PROCESSING, aggregationObject);

setInterval(deleteFolder, IntervalTime.DELETE_FOLDER, localPath);
setInterval(deleteFolder, IntervalTime.DELETE_FOLDER, `${localPath}_gz`);

setInterval(unprocessedS3Files, IntervalTime.FAILED_FILES, IFolder.FAILED);
setInterval(unprocessedS3Files, IntervalTime.UNPROCESSED_FILES, IFolder.UNPROCESSED);
setInterval(processedS3FilesCleanUp, IntervalTime.CLEAN_UP_PROCESSED_FILES, IFolder.PROCESSED);

httpServer.listen(port, host, (): void => {
  consola.success(`Server is running on http://${host}:${port} NODE_ENV:${process.env.NODE_ENV} Using node - { ${process.version} }`);
  consola.info(`S3_BUCKET_NAME:${process.env.S3_BUCKET_NAME}, AWS_ACCESS_KEY_ID:${process.env.AWS_ACCESS_KEY_ID}`);
});
