--
-- table
--
create table {SCHEMA}tag_adsets (
    company_identifier character varying(381),
    app character varying(381),
    channel character varying(381) NOT NULL,
    adset_id character varying(765) NOT NULL,
    adset_tag character varying(6138),
    adset_subtag character varying(765),
    upload_group character varying(381),
    PRIMARY KEY (channel, adset_id)
)
distkey (adset_id)
sortkey (channel, adset_id);
