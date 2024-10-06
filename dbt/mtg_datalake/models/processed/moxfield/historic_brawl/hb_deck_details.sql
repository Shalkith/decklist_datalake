with 
decks as (
select id ,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.name')) as name,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.publicUrl')) as publicurl,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.likeCount')) as likecount,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.viewCount')) as viewcount,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.commentCount')) as commentcount,
JSON_UNQUOTE(JSON_EXTRACT(deckdata, '$.lastUpdatedAtUtc')) as lastupdated
from {{ref('historicbrawl_decks_history')}} hdh where dbt_valid_to is null 
),
commanders as (
select id,card from {{ref('hb_board_commanders')}}
)
SELECT d.id,
c.card as commander,
d.name,
d.publicurl,
d.likecount,
d.viewcount,
d.commentcount,
d.lastupdated
FROM decks d 
left join commanders c on d.id = c.id
