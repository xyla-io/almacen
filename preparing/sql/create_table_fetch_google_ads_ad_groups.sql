--
-- table
--
create table {SCHEMA}fetch_google_ads_ad_groups (
    ad_group_id character varying(765) not null,
    ad_group_name character varying(6138),
    ad_group_status character varying(765),
    ad_group_type character varying(765),
    ad_group_target_cpa_micros double precision,
    ad_group_target_cpm_micros double precision,
    campaign_id character varying(765) not null,
    campaign_name character varying(6138),
    campaign_status character varying(765),
    campaign_advertising_channel_type character varying(765),
    campaign_advertising_channel_sub_type character varying(765),
    campaign_network_settings_target_content_network character varying(765),
    campaign_network_settings_target_partner_search_network character varying(765),
    campaign_start_date date not null,
    campaign_serving_status character varying(765),
    campaign_bidding_strategy_type character varying(765),
    campaign_target_cpa_target_cpa_micros double precision,
    campaign_app_campaign_setting_bidding_strategy_goal_type character varying(765),
    campaign_app_campaign_setting_app_id character varying(189),
    campaign_app_campaign_setting_app_store character varying(189),
    metrics_clicks bigint,
    metrics_conversions double precision,
    metrics_cost_micros double precision,
    metrics_impressions bigint,
    metrics_interactions bigint,
    metrics_conversions_value double precision,
    metrics_interaction_event_types text,
    metrics_video_views bigint,
    metrics_all_conversions double precision,
    segments_device character varying(765),
    segments_date date not null,
    cost double precision,
    customer_id character varying(765),
    customer_descriptive_name character varying(765),
    latest_customer_descriptive_name character varying(765),
    customer_currency_code character varying(9),
    extracted_product character varying(765),
    product character varying(765),
    product_id character varying(189),
    product_name character varying(765),
    product_platform character varying(765),
    product_os character varying(189),
    latest_campaign_name character varying(6138),
    latest_ad_group_name character varying(6138),
    converted_currency character varying(9),
    currency_conversion_rate double precision,
    company_display_name character varying(765) not null,
    crystallized bool NOT NULL default true,
    fetch_date datetime,
    PRIMARY KEY (campaign_id, segments_date)
)
distkey (segments_date)
sortkey (segments_date, ad_group_id);