--
-- table
--
create table {SCHEMA}entity_ad_materialized (
  channel character varying(765),
  campaign_id character varying(765),
  adset_id character varying(765),
  ad_id character varying(765),
  ad_name character varying(6138),
  ad_type character varying(189),
  ad_status character varying(189),
  product_id character varying(765),
  product_name character varying(765),
  product_platform character varying(189),
  last_active_date date,
  creative_url character varying(6138),
  fetch_date date,
  PRIMARY KEY (channel, campaign_id, adset_id, ad_id)
)
distkey (ad_id)
sortkey (channel, campaign_id, adset_id, ad_id);
