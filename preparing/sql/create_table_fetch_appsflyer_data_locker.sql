--
-- table
--
CREATE TABLE {SCHEMA}fetch_appsflyer_data_locker (
    app_id character varying(765),
    bundle_id character varying(765),
    app_name character varying(6138),
    app_version character varying(765),
    agency character varying(765),
    media_source character varying(765),
    channel character varying(765),
    campaign character varying(6138),
    adset character varying(6138),
    ad character varying(6138),
    campaign_id character varying(765),
    adset_id character varying(765),
    ad_id character varying(765),
    keyword character varying(6138),
    keyword_match_type character varying(765),
    country_code character varying(765),
    is_primary_attribution bool,
    is_retargeting bool,
    retargeting_conversion_type character varying(765),
    platform character varying(765) not null,
    event_name character varying(765) not null,
    attributed_touch_date date,
    install_date date not null,
    event_date date not null,
    event_day int,
    revenue double precision,
    events bigint,
    company_display_name character varying(765),
    data_locker_report_type character varying(765),
    data_locker_date date not null,
    data_locker_hour int,
    data_locker_start_part int not null,
    data_locker_end_part int not null,
    data_locker_last_part int not null,
    effective_date date not null,
    crystallized bool NOT NULL default true,
    event_date_crystallized bool NOT NULL default false,
    effective_date_crystallized bool NOT NULL default false,
    fetch_date datetime
)
distkey (event_date)
sortkey (event_date, ad_id);
