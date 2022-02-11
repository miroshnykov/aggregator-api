export interface ITraffic {
  lid: string,
  affiliate_id: number
  campaign_id: number
  offer_id: number
  offer_name: string
  offer_type: string
  offer_description: string
  landing_page: string
  landing_page_id: number
  payin: number
  payout: number
  geo: string
  cap_override_offer_id: number
  is_cpm_option_enabled: number | boolean
  landing_page_id_origin: number
  landing_page_url_origin: string
  advertiser_id: number
  advertiser_manager_id: number
  affiliate_manager_id: number
  origin_advertiser_id: number
  origin_conversion_type: string
  origin_is_cpm_option_enabled: number | boolean
  origin_offer_id: number
  origin_vertical_id: number
  verticals: number
  vertical_name: string
  conversion_type: string
  platform: string
  payout_percent: number
  device: string
  os: string
  isp: string
  date_added: number
  click: number
  event_type: string
  referer?: string
  fingerprint?: string
  is_unique_visit?: boolean
}

export interface IBonusLidRes {
  success: boolean,
  time: number
  errors?: string
}
