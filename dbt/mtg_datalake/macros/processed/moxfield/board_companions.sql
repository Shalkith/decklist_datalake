
{% macro board_companions(tablename) %}

with 
decks as (
SELECT id,lastUpdatedAtUtc,
	format,
    JSON_UNQUOTE(JSON_EXTRACT(companions, CONCAT('$.cards.', `key`, '.card.name'))) AS card,
    JSON_UNQUOTE(JSON_EXTRACT(companions, CONCAT('$.cards.', `key`, '.quantity '))) AS quantity   
FROM {{ref(tablename+'_decks_history')}},
  JSON_TABLE(
    JSON_KEYS(JSON_EXTRACT(companions, '$.cards')),
    '$[*]' COLUMNS (`key` VARCHAR(255) PATH '$')
  ) AS t
)
SELECT id as deck_id,lastUpdatedAtUtc,card as card_name, format,quantity
FROM decks


{% endmacro %}