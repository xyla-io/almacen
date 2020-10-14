--
-- table
--
create table {SCHEMA}tag_ads (
    company_identifier character varying(381),
    app character varying(381),
    channel character varying(381) NOT NULL,
    ad_id character varying(765) NOT NULL,
    ad_tag character varying(6138),
    ad_subtag character varying(765),
    upload_group character varying(381),
    PRIMARY KEY (channel, ad_id)
)
distkey (ad_id)
sortkey (channel, ad_id);
