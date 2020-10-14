--
-- function
--
CREATE OR REPLACE FUNCTION {SCHEMA}appsflyer_media_source_channel(character varying) RETURNS character varying IMMUTABLE
  AS $$
    SELECT CASE $1
        WHEN 'Apple Search Ads' THEN 'Apple'
        WHEN 'Facebook Ads' THEN 'Facebook'
        WHEN 'googleadwords_int' THEN 'Google'
        WHEN 'snapchat_int' THEN 'Snapchat'
        WHEN 'bytedanceglobal_int' THEN 'TikTok'
        ELSE ''
    END
$$ LANGUAGE sql
