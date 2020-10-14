--
-- table
--
CREATE TABLE {SCHEMA}fetch_adjust_deliverables (
    tracker_token character varying(189) not null,
    campaign character varying(6138),
    adgroup character varying(6138),
    creative character varying(6138),
    os_name character varying(189) not null,
    event_token character varying(189) not null,
    event_name character varying(3066),
    campaign_name character varying(6138),
    adgroup_name character varying(6138),
    creative_name character varying(6138),
    campaign_id character varying(765),
    adgroup_id character varying(765),
    creative_id character varying(765),
    network character varying(765),
    country character varying(189),
    region character varying(189),
    date date NOT NULL,
    clicks bigint,
    impressions bigint,
    installs bigint,
    uninstalls bigint,
    reinstalls bigint,
    click_conversion_rate double precision,
    ctr double precision,
    impression_conversion_rate double precision,
    reattributions bigint,
    reattribution_reinstalls bigint,
    deattributions bigint,
    sessions bigint,
    events bigint,
    first_events bigint,
    revenue_events bigint,
    revenue double precision,
    daus double precision,
    waus double precision,
    maus double precision,
    limit_ad_tracking_installs bigint,
    limit_ad_tracking_install_rate double precision,
    limit_ad_tracking_reattributions double precision,
    limit_ad_tracking_reattribution_rate double precision,
    gdpr_forgets bigint,
    company_display_name character varying(765),
    app_display_name character varying(765),
    app_token character varying(189) not null,
    crystallized bool NOT NULL,
    fetch_date datetime,
    PRIMARY KEY (app_token, tracker_token, os_name, event_token, date)
)
distkey ("date")
sortkey ("date", creative_id);