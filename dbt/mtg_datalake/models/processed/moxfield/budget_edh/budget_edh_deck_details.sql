with 
decks as (
select id ,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.name')) as deck_name,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.description')) as description,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.publicUrl')) as publicurl,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.likeCount')) as likecount,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.viewCount')) as viewcount,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.commentCount')) as commentcount,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.colors')) as colors,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.lastUpdatedAtUtc')) as lastupdated,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.createdAtUtc')) as createdat,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.hubs'))  as hubs
from {{ref('budget_edh_decks_history')}} hdh where dbt_valid_to is null 
),
commanders as (
    select deck_id,GROUP_CONCAT(card_name order by card_name asc SEPARATOR '[and]') as card_name from {{ref('budget_edh_board_commanders')}}
    GROUP BY deck_id 
),
distinct_cards as (
    select deck_id,count(distinct card_name) count from {{ref('budget_edh_decklists')}}
    group by 1
)
SELECT d.id as deck_id,
c.card_name as commander_name,
d.deck_name,
description,
dc.count+0 as card_count,
d.colors,
d.publicurl,
d.likecount,
d.viewcount,
d.commentcount,
d.lastupdated,
d.createdat,
hubs
FROM decks d 
left join commanders c on d.id = c.deck_id
left join distinct_cards dc on c.deck_id = dc.deck_id