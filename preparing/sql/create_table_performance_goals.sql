--
-- table
--
create table {SCHEMA}performance_goals (
    goal_name character varying(765) NOT NULL default '',
    goal_entity character varying(765) NOT NULL default '',
    goal_event character varying(765) NOT NULL default '',
    goal_metric character varying(765) NOT NULL default '',
    goal_granularity character varying(765) NOT NULL default '',
    goal_value double precision,
    goal_country_code character varying(765) NOT NULL default '',
    goal_tag character varying(765) NOT NULL default '',
    goal_subtag character varying(765) NOT NULL default '',
    goal_channel character varying(765) NOT NULL default '',
    goal_mmp character varying(765) NOT NULL default '',
    goal_platform character varying(765) NOT NULL default '',
    product_display_name character varying(765) NOT NULL default '',
    product_id character varying(765) NOT NULL default ''
)
distkey (goal_name)
sortkey (goal_tag, goal_subtag);