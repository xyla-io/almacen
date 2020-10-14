--
-- function
--
CREATE OR REPLACE FUNCTION {SCHEMA}adjust_network_channel (character varying) returns character varying
IMMUTABLE
AS $$
  SELECT CASE
    WHEN $1 = 'Apple Search Ads' THEN 'Apple'
    WHEN $1 = 'iAd Installs' THEN 'Apple'
    WHEN $1 = 'Organic' THEN 'organic'
    WHEN $1 LIKE '%%Facebook%%' THEN 'Facebook'
    WHEN $1 LIKE '%%Instagram%%' THEN 'Facebook'
    WHEN $1 LIKE '%%Adwords%%' THEN 'Google'
    WHEN $1 LIKE '%%Google%%' THEN 'Google'
    WHEN $1 LIKE '%%Snapchat%%' THEN 'Snapchat'
    WHEN $1 LIKE '%%TikTok%%' THEN 'TikTok'
    ELSE ''
    END
$$ language sql;
