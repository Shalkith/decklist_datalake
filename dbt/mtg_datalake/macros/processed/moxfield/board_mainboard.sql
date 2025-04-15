
{% macro board_mainboard(tablename) %}

with 
pre_filter as (
  select * from {{ref(tablename+'_decks_history')}}
WHERE dbt_valid_to IS NULL 
 {% if is_incremental() %}
    AND lastupdated > (SELECT date_add(max(lastupdated),interval -5 day) FROM {{this}})
 {% else %}
     AND lastupdated < '2025-01-01'
{% endif %}
),
decks as (
SELECT id,lastupdated,
	JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.format')) as format,
    JSON_UNQUOTE(JSON_EXTRACT(deckdata, CONCAT('$.boards.mainboard.cards.', `key`, '.card.name'))) AS card,
    JSON_UNQUOTE(JSON_EXTRACT(deckdata, CONCAT('$.boards.mainboard.cards.', `key`, '.quantity'))) AS quantity   
FROM pre_filter,
  JSON_TABLE(
    JSON_KEYS(JSON_EXTRACT(deckdata, '$.boards.mainboard.cards')),
    '$[*]' COLUMNS (`key` VARCHAR(255) PATH '$')
  ) AS t
)
SELECT id as deck_id,lastupdated,card as card_name, format,quantity
FROM decks

{% endmacro %}