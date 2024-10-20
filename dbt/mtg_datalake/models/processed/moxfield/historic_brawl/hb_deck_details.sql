with 
decks as (
select id ,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.name')) as name,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.description')) as description,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.publicUrl')) as publicurl,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.likeCount')) as likecount,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.viewCount')) as viewcount,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.commentCount')) as commentcount,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.colors')) as colors,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.lastUpdatedAtUtc')) as lastupdated,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.createdAtUtc')) as createdat,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.hubs'))  as hubs
from {{ref('historicbrawl_decks_history')}} hdh where dbt_valid_to is null 
),
commanders as (
select id,card from {{ref('hb_board_commanders')}}
),
distinct_cards as (
    select id,count(distinct card) count from {{ref('hb_decklists')}}
    group by 1
)
SELECT d.id,
c.card as commander,
d.name,
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
left join commanders c on d.id = c.id
left join distinct_cards dc on c.id = dc.id