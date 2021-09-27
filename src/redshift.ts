import {Client, Pool} from "pg"

export const redshiftClient = new Client({
  user: process.env.REDSHIFT_USER,
  host: process.env.REDSHIFT_HOST,
  database: process.env.REDSHIFT_DATABASE,
  password: process.env.REDSHIFT_PASSWORD,
  port: Number(process.env.REDSHIFT_PORT || 5439),
})

export const pool = new Pool({
  user: process.env.REDSHIFT_USER,
  host: process.env.REDSHIFT_HOST,
  database: process.env.REDSHIFT_DATABASE,
  password: process.env.REDSHIFT_PASSWORD,
  port: Number(process.env.REDSHIFT_PORT || 5439),
})

