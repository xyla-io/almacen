--
-- table
--
create table {SCHEMA}fetch_google_ads_assets (
  customer_id character varying(765),
  customer_descriptive_name character varying(765),
  latest_customer_descriptive_name character varying(765),
  customer_currency_code character varying(9),
  asset_id character varying(189),
  asset_name character varying(765),
  asset_type character varying(189),
  asset_resource_name character varying(189),
  asset_image_asset_file_size bigint,
  asset_image_asset_full_size_url character varying(6138),
  asset_image_asset_full_size_width_pixels int,
  asset_image_asset_full_size_height_pixels int,
  asset_image_asset_mime_type character varying(189),
  asset_text_asset_text character varying(6138),
  asset_youtube_video_asset_youtube_video_id character varying(189),
  company_display_name character varying(765) not null,
  crystallized bool NOT NULL default true,
  fetch_date datetime,
  PRIMARY KEY (customer_id, asset_id)
)
distkey (asset_id)
sortkey (customer_id, asset_id);
