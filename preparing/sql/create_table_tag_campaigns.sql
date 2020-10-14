--
-- table
--
create table {SCHEMA}tag_campaigns (
    company_identifier character varying(381) NOT NULL,
    app character varying(381) NOT NULL,
    channel character varying(381) NOT NULL,
    campaign_id character varying(765) NOT NULL,
    campaign_tag character varying(765),
    campaign_subtag character varying(765),
    upload_group character varying(381),
    PRIMARY KEY (channel, campaign_id)
)
distkey (campaign_id)
sortkey (channel, campaign_id);
