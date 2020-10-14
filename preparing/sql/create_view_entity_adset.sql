--
-- view
--
create or replace view {SCHEMA}entity_adset as
  select * 
  from {SCHEMA}entity_adset_materialized
  left join (
    select channel, campaign_id, campaign_name, campaign_status, campaign_type
    from {SCHEMA}entity_campaign_materialized
  )
  using (channel, campaign_id)
  left join (
    select channel, campaign_id, campaign_tag, campaign_subtag
    from {SCHEMA}tag_campaigns
  )
  using (channel, campaign_id)
  left join (
    select channel, adset_id, adset_tag, adset_subtag
    from {SCHEMA}tag_adsets
  )
  using (channel, adset_id)
