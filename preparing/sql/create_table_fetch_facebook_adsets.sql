--
-- table
--
create table {SCHEMA}fetch_facebook_adsets (
    account_id character varying(765) NOT NULL,
    account_name character varying(765),
    campaign_id character varying(765) NOT NULL,
    campaign_name character varying(6138),
    campaign_objective character varying(765),
    adset_id character varying(765) NOT NULL,
    adset_name character varying(6138),
    actions character varying(32768),
    clicks numeric(28,6),
    frequency double precision,
    date_start date not null,
    date_stop date not null,
    impressions numeric(28,6),
    spend double precision,
    unique_clicks numeric(28,6),
    company_display_name character varying(765),
    app_display_name character varying(765),
    user_os character varying(6138),
    platform character varying(765) not null,
    extracted_product character varying(765),
    extracted_product_id character varying(189),
    extracted_product_platform character varying(189),
    product character varying(765),
    product_id character varying(189),
    product_name character varying(765),
    product_platform character varying(765),
    product_os character varying(189),
    latest_campaign_name character varying(6138),
    latest_adset_name character varying(6138),
    account_currency character varying(9),
    converted_currency character varying(9),
    currency_conversion_rate double precision,
    crystallized bool NOT NULL default true,
    fetch_date datetime,
    PRIMARY KEY (adset_id, platform, date_start)
)
distkey (date_start)
sortkey (date_start, adset_id);
