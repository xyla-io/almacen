--
-- view
--
create or replace view {SCHEMA}entity_campaign as
  select * 
  from {SCHEMA}entity_campaign_materialized
  left join (
    select channel, campaign_id, campaign_tag, campaign_subtag
    from {SCHEMA}tag_campaigns
  )
  using (channel, campaign_id)