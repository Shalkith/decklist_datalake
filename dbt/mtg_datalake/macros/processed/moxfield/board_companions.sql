
{% macro board_companions(tablename) %}
with 
decks as (
SELECT id,lastupdated,
	JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.format')) as format,
    JSON_UNQUOTE(JSON_EXTRACT(deckdata, CONCAT('$.boards.companions.cards.', `key`, '.card.name'))) AS card,
    JSON_UNQUOTE(JSON_EXTRACT(deckdata, CONCAT('$.boards.companions.cards.', `key`, '.quantity'))) AS quantity   
FROM {{ref(tablename+'_decks_history')}},
  JSON_TABLE(
    JSON_KEYS(JSON_EXTRACT(deckdata, '$.boards.companions.cards')),
    '$[*]' COLUMNS (`key` VARCHAR(255) PATH '$')
  ) AS t
WHERE dbt_valid_to IS NULL
 {% if is_incremental() %}
    AND lastupdated > (SELECT MAX(lastupdated) FROM {{this}})
 {% endif %} 
)
SELECT id as deck_id,lastupdated,card as card_name, format,quantity
FROM decks

{% endmacro %}