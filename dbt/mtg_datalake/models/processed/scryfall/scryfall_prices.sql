

with prices as (
SELECT name, 
       JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(REPLACE(prices, '''', '"'), 'None', 'null'), '"{', '{'),'$.eur')) AS eur,
       JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(REPLACE(prices, '''', '"'), 'None', 'null'), '"{', '{'),'$.eur_foil')) AS eur_foil,
       JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(REPLACE(prices, '''', '"'), 'None', 'null'), '"{', '{'),'$.usd')) AS usd,
       JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(REPLACE(prices, '''', '"'), 'None', 'null'), '"{', '{'),'$.usd_etched')) AS usd_etched,
       JSON_UNQUOTE(JSON_EXTRACT(REPLACE(REPLACE(REPLACE(prices, '''', '"'), 'None', 'null'), '"{', '{'),'$.usd_foil')) AS usd_foil
FROM {{ref('oracle_history')}}
WHERE dbt_valid_to IS NULL),
final as (
SELECT *,
       LEAST(
           COALESCE(NULLIF(eur, 'null'), 999999999), 
           COALESCE(NULLIF(eur_foil, 'null'), 999999999)
       ) AS min_eur,
       GREATEST(
           COALESCE(NULLIF(eur, 'null'), -999999999), 
           COALESCE(NULLIF(eur_foil, 'null'), -999999999)
       ) AS max_eur,
       LEAST(
           COALESCE(NULLIF(usd, 'null'), 999999999), 
           COALESCE(NULLIF(usd_etched, 'null'), 999999999), 
           COALESCE(NULLIF(usd_foil, 'null'), 999999999)
       ) AS min_usd,
       GREATEST(
           COALESCE(NULLIF(usd, 'null'), -999999999), 
           COALESCE(NULLIF(usd_etched, 'null'), -999999999), 
           COALESCE(NULLIF(usd_foil, 'null'), -999999999)
       ) AS max_usd
FROM prices
)
select * from final 