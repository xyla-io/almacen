--
-- table
--
create table {SCHEMA}entity_campaign_materialized (
  channel character varying(765),
  campaign_id character varying(765),
  campaign_name character varying(6138),
  campaign_type character varying(189),
  campaign_status character varying(189),
  product_id character varying(765),
  product_name character varying(765),
  product_platform character varying(189),
  last_active_date date,
  fetch_date date,
  PRIMARY KEY (channel, campaign_id)
)
distkey (campaign_id)
sortkey (channel, campaign_id);
