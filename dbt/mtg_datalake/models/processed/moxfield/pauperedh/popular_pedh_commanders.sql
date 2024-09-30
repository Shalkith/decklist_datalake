
with decks as (
SELECT id, 
  GROUP_CONCAT(
    JSON_UNQUOTE(JSON_EXTRACT(deckdata, CONCAT('$.boards.commanders.cards.', `key`, '.card.name'))) 
    ORDER BY JSON_UNQUOTE(JSON_EXTRACT(deckdata, CONCAT('$.boards.commanders.cards.', `key`, '.card.name'))) SEPARATOR ' & '
  ) AS commander_names
FROM `mtg_datalake`.`pauperedh_decks_history`,
  JSON_TABLE(
    JSON_KEYS(JSON_EXTRACT(deckdata, '$.boards.commanders.cards')),
    '$[*]' COLUMNS (`key` VARCHAR(255) PATH '$')
  ) AS t
WHERE dbt_valid_to IS NULL 
--  AND id = 'NERQ9CQtn0K_5wwmnhLc4A'
GROUP BY id
)

SELECT commander_names, COUNT(*) AS deck_count
FROM decks
GROUP BY commander_names
ORDER BY deck_count DESC


