import { DynamoDBClient } from '@aws-sdk/client-dynamodb';
import { PutCommand } from '@aws-sdk/lib-dynamodb';
import consola from 'consola';
import { influxdb } from './metrics';
import { ILid } from './Interfaces/lid';

// eslint-disable-next-line consistent-return
export const sendLidDynamoDb = async (lidInfoToSend: ILid) => {
  try {
    const dynamoDbConf = {
      region: process.env.AWS_DYNAMODB_REGION,
    };
    const ddbClient: DynamoDBClient = new DynamoDBClient(dynamoDbConf);
    const leadParams = {
      TableName: process.env.AWS_DYNAMODB_TABLE_NAME,
      Item: lidInfoToSend,
    };
    return await new Promise<boolean>((resolve, reject) => {
      ddbClient.send(new PutCommand(leadParams), (err: Error) => {
        if (err) {
          consola.error('set Data to DynamoDb Error:', err);
          reject();
        }

        consola.info(`lid ${lidInfoToSend.lid} was created successfully `);
        resolve(true);
      });
    });
  } catch (e) {
    consola.error('sendLidDynamoDb:', e);
    influxdb(500, 'dynamo_db_create_lid_error');
  }
};
