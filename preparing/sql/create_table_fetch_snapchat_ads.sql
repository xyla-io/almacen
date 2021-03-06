--
-- table
--
create table {SCHEMA}fetch_snapchat_ads (
    ad_account_id character varying(765) NOT NULL,
    ad_account_timezone character varying(189) NOT NULL,
    app_display_name character varying(765) NOT NULL,
    ad_id character varying(765) NOT NULL,
    ad_name character varying(6138),
    ad_squad_id character varying(765),
    ad_status character varying(189),
    ad_type character varying(189),
    creative_id character varying(765),
    campaign_id character varying(765) NOT NULL,
    campaign_name character varying(6138) NOT NULL,
    campaign_status character varying(189) NOT NULL,
    campaign_objective character varying(765),
    campaign_daily_budget double precision,
    campaign_lifetime_spend_cap double precision,
    campaign_android_app_url character varying(189),
    campaign_ios_app_id character varying(189),
    ad_squad_name character varying(6138),
    ad_squad_status character varying(189),
    ad_squad_placement character varying(765),
    ad_squad_optimization_goal character varying(189),
    ad_squad_daily_budget double precision,
    ad_squad_auto_bid character varying(15),
    ad_squad_bid double precision,
    ad_squad_billing_event character varying(765),
    ad_squad_lifetime_budget double precision,
    os_types character varying(6138),
    platform character varying(189),
    start_time date NOT NULL,
    end_time date NOT NULL,
    impressions numeric(28,6),
    quartile_1 numeric(28,6),
    quartile_2 numeric(28,6),
    quartile_3 numeric(28,6),
    screen_time_millis numeric(28,6),
    spend double precision,
    swipes numeric(28,6),
    video_views numeric(28,6),
    view_completion numeric(28,6),
    frequency double precision,
    uniques numeric(28,6),
    swipe_up_percent double precision,
    company_display_name character varying(765) NOT NULL,
    account_currency character varying(9) NOT NULL,
    converted_currency character varying(9),
    extracted_product character varying(765),
    extracted_product_id character varying(189),
    product character varying(765),
    product_id character varying(189),
    product_name character varying(765),
    product_platform character varying(765),
    product_os character varying(189),
    latest_campaign_name character varying(6138),
    currency_conversion_rate double precision,
    crystallized bool NOT NULL default true,
    fetch_date datetime,
    PRIMARY KEY (ad_id, start_time)
)
distkey (start_time)
sortkey (start_time, ad_id);
