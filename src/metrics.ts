import Influx from 'influxdb-nodejs';
import os from 'os';
import consola from 'consola';
import * as dotenv from 'dotenv';
import * as _ from 'lodash';

dotenv.config();
const host = process.env.GRAFANA_HOST;
const clientInfluxdb = new Influx(host);

const projectName = process.env.GRAFANA_PROJECT_NAME;
const project = (process.env.NODE_ENV === 'development' || process.env.NODE_ENV === 'staging') ? `${projectName}_staging` : projectName;
const hostname = os.hostname();

enum Interval {
  INTERVAL_REQUEST = 10,
  INTERVAL_SYSTEMS = 30000,
}

interface IParam {
  code: number
  route: string
  method: string
}

consola.info(`Grafana project name:${project}, host:${host}`);
export const influxdb = (statusCode: number, route: string) => {
  const params: IParam = {
    code: statusCode,
    route,
    method: 'GET',
  };

  clientInfluxdb.write(`${project}_request`)
    .tag({
      project,
      host: hostname,
      route: params.route,
      method: params.method,
      status: _.sortedIndex([99, 199, 299, 399, 499, 599], params.code) * 100,
    })
    .field(params)
    .time(Date.now(), 'ms')
    .queue();

  if (clientInfluxdb.writeQueueLength >= Interval.INTERVAL_REQUEST) {
    // consola.success(`Send to Grafana, interval:${Interval.INTERVAL_REQUEST} `)
    clientInfluxdb.syncWrite()
      .catch((error: any) => {
        consola.error(error);
      });
  }
};
