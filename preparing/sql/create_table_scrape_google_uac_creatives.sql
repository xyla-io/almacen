--
-- table
--
create table {SCHEMA}scrape_google_uac_creatives (
    ad_id character varying(765) not null,
    status character varying(765),
    type character varying(765),
    title character varying(765),
    metadata character varying(6138),
    media_source_url character varying(6138),
    thumbnail_url character varying(6138),
    performance_group character varying(765),
    conversion_action character varying(765) not null,
    clicks numeric(28,6),
    impressions numeric(28,6),
    click_through_rate double precision,
    cost_per_click double precision,
    cost double precision,
    cost_per_conversion double precision,
    conversion_rate double precision,
    conversions double precision,
    conversion_value_per_cost double precision,
    conversion_value_per_click double precision,
    value_per_conversion double precision,
    conversion_value double precision,
    average_position double precision,
    campaign_id character varying(765) not null,
    campaign_name character varying(6138),
    date date not null,
    company_display_name character varying(765),
    app_display_name character varying(765),
    platform character varying(189) not null,
    fetch_date timestamp without time zone NOT NULL,
    configuration character varying(765) NOT NULL,
    crystallized bool NOT NULL default true,
    PRIMARY KEY (ad_id, conversion_action, "date")
)
distkey ("date")
sortkey ("date", ad_id);
