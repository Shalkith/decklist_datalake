with 
decks as (
SELECT id,
	JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.format')) as format,
    JSON_UNQUOTE(JSON_EXTRACT(deckdata, CONCAT('$.boards.companions.cards.', `key`, '.card.name'))) AS card,
    JSON_UNQUOTE(JSON_EXTRACT(deckdata, CONCAT('$.boards.companions.cards.', `key`, '.quantity'))) AS quantity   
FROM {{ref('pauperedh_decks_history')}},
  JSON_TABLE(
    JSON_KEYS(JSON_EXTRACT(deckdata, '$.boards.companions.cards')),
    '$[*]' COLUMNS (`key` VARCHAR(255) PATH '$')
  ) AS t
WHERE dbt_valid_to IS NULL 
)
SELECT id,card, format,quantity
FROM decks
