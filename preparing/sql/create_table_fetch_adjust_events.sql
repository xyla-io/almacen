--
-- table
--
CREATE TABLE {SCHEMA}fetch_adjust_events (
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
    revenue_events bigint,
    revenue double precision,
    events bigint,
    first_events bigint,
    company_display_name character varying(765),
    app_display_name character varying(765),
    app_token character varying(189) not null,
    crystallized bool NOT NULL,
    fetch_date datetime,
    PRIMARY KEY (app_token, tracker_token, os_name, event_token, date)
)
distkey ("date")
sortkey ("date", creative_id);
