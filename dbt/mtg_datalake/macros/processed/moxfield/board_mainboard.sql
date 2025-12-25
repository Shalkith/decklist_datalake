
{% macro board_mainboard(tablename) %}
depends_on: {{ ref('historicbrawl_decks_history') }}

with 
decks as (
SELECT id,lastUpdatedAtUtc,
	format,
    JSON_UNQUOTE(JSON_EXTRACT(mainboard, CONCAT('$.cards.', `key`, '.card.name'))) AS card,
    JSON_UNQUOTE(JSON_EXTRACT(mainboard, CONCAT('$.cards.', `key`, '.quantity'))) AS quantity   
FROM {{ref(tablename+'_decks_history')}},
  JSON_TABLE(
    JSON_KEYS(JSON_EXTRACT(mainboard, '$.cards')),
    '$[*]' COLUMNS (`key` VARCHAR(255) PATH '$')
  ) AS t
)
SELECT id as deck_id,lastUpdatedAtUtc,card as card_name, format,quantity
FROM decks

{% endmacro %}