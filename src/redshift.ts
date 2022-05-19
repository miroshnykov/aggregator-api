import { Client, Pool } from 'pg';
import consola from 'consola';
import { ITraffic } from './Interfaces/traffic';
import { influxdb } from './metrics';

export const redshiftClient = new Client({
  user: process.env.REDSHIFT_USER,
  host: process.env.REDSHIFT_HOST,
  database: process.env.REDSHIFT_DATABASE,
  password: process.env.REDSHIFT_PASSWORD,
  port: Number(process.env.REDSHIFT_PORT || 5439),
});

// consola.info(`REDSHIFT_HOST: { ${process.env.REDSHIFT_HOST} } REDSHIFT_USER: { ${process.env.REDSHIFT_USER} }  REDSHIFT_PORT: { ${process.env.REDSHIFT_PORT} } REDSHIFT_TABLE: { ${process.env.REDSHIFT_TABLE} } REDSHIFT_DATABASE: { ${process.env.REDSHIFT_DATABASE} } REDSHIFT_SCHEMA: { ${process.env.REDSHIFT_SCHEMA} }`);

export const pool = new Pool({
  user: process.env.REDSHIFT_USER,
  host: process.env.REDSHIFT_HOST,
  database: process.env.REDSHIFT_DATABASE,
  password: process.env.REDSHIFT_PASSWORD,
  port: Number(process.env.REDSHIFT_PORT || 5439),
});

export const selectLid = async (lid: string) => {
  const client = await pool.connect();
  const query = `SELECT  lid,
                         affiliate_id,
                         campaign_id,
                         offer_id,
                         offer_name,
                         offer_type,
                         offer_description,
                         landing_page,
                         landing_page_id,
                         payin,
                         payout,
                         geo,
                         cap_override_offer_id,
                         is_cpm_option_enabled,
                         landing_page_id_origin,
                         advertiser_id,
                         advertiser_manager_id,
                         affiliate_manager_id,
                         origin_advertiser_id,
                         origin_conversion_type,
                         origin_is_cpm_option_enabled,
                         origin_offer_id,
                         origin_vertical_id,
                         verticals,
                         vertical_name,
                         conversion_type,
                         platform,
                         payout_percent,
                         device,
                         os,
                         isp,
                         date_added,
                         click,
                         referer,
                         event,
                         fingerprint,
                         is_unique_visit  
                   FROM ${process.env.REDSHIFT_SCHEMA}.traffic
                   where lid='${lid}'`;

  const lidData = await client.query(query);
  client.release();
  return lidData.rows.length !== 0 && lidData.rows[0];
};

export const insertBonusLid = async (data: ITraffic) => {
  const client = await pool.connect();
  /* eslint-disable */
  const {
    lid,
    affiliate_id,
    campaign_id,
    offer_id,
    offer_name,
    offer_type,
    offer_description,
    landing_page,
    landing_page_id,
    payin,
    payout,
    geo,
    cap_override_offer_id,
    is_cpm_option_enabled,
    landing_page_id_origin,
    landing_page_url_origin,
    advertiser_id,
    advertiser_manager_id,
    affiliate_manager_id,
    origin_advertiser_id,
    origin_conversion_type,
    origin_is_cpm_option_enabled,
    origin_offer_id,
    origin_vertical_id,
    verticals,
    vertical_name,
    conversion_type,
    platform,
    payout_percent,
    device,
    os,
    isp,
    date_added,
    click,
    event_type,
    event,
    referer,
    fingerprint,
    is_unique_visit,
  } = data;
  /* eslint-enable */
  try {
    const timeCurrent: number = new Date().getTime();
    const dateAdd = Math.floor(timeCurrent / 1000);
    const sql = `INSERT INTO ${process.env.REDSHIFT_SCHEMA}.traffic(  lid,
                                                                      affiliate_id,
                                                                      campaign_id,
                                                                      offer_id,
                                                                      offer_name,
                                                                      offer_type,
                                                                      offer_description,
                                                                      landing_page,
                                                                      landing_page_id,
                                                                      payin,
                                                                      payout,
                                                                      geo,
                                                                      cap_override_offer_id,
                                                                      is_cpm_option_enabled,
                                                                      landing_page_id_origin,
                                                                      advertiser_id,
                                                                      advertiser_manager_id,
                                                                      affiliate_manager_id,
                                                                      origin_advertiser_id,
                                                                      origin_conversion_type,
                                                                      origin_is_cpm_option_enabled,
                                                                      origin_offer_id,
                                                                      origin_vertical_id,
                                                                      verticals,
                                                                      vertical_name,
                                                                      conversion_type,
                                                                      platform,
                                                                      payout_percent,
                                                                      device,
                                                                      os,
                                                                      isp,
                                                                      date_added,
                                                                      click,
                                                                      referer,
                                                                      event,
                                                                      fingerprint,
                                                                      is_unique_visit)
             VALUES (   '${lid}',
                        ${affiliate_id},
                        ${campaign_id},
                        ${offer_id},
                        '${offer_name}',
                        '${offer_type}',
                        '${offer_description}',
                        '${landing_page}',
                        ${landing_page_id},
                        ${payin},
                        ${payout},
                        '${geo}',
                        ${cap_override_offer_id},
                        ${is_cpm_option_enabled},
                        ${landing_page_id_origin},
                        ${advertiser_id},
                        ${advertiser_manager_id},
                        ${affiliate_manager_id},
                        ${origin_advertiser_id},
                        '${origin_conversion_type}',
                        ${origin_is_cpm_option_enabled},
                        ${origin_offer_id},
                        ${origin_vertical_id},
                        ${verticals},
                        '${vertical_name}',
                        '${conversion_type}',
                        '${platform}',
                        ${payout_percent},
                        '${device}',
                        '${os}',
                        '${isp}',
                        ${dateAdd},
                        ${click},
                        '${referer}',
                        '${event}',
                        '${fingerprint}',
                        ${is_unique_visit}
        );
    `;

    await client.query(sql);
    client.release();
    influxdb(200, 'insert_bonus_lid_redshift_success');
    return true;
  } catch (e) {
    consola.error('insertBonusLidError:', e);
    influxdb(500, 'insert_bonus_lid_redshift_error');
    return false;
  }
};
