
with 
decks as (
select * from {{ref('hb_decklists')}}
),
mycards as (select name, 
case when name like 'Snow-Covered%' then 99 else quantity end as quantity 
from {{ref('mtga_collection')}} mc ),
combined as (
select d.*,m.name from decks d left join mycards m on d.card = m.name and d.quantity <= m.quantity
),
quantitycheck as (
select distinct id, sum(quantity) quantity from combined 
where name is not NULL 
group by 1
having sum(quantity) <= 100
)
select q.id,d.commander, d.name as deckname, q.quantity as percent_complete,
(d.commentcount * 1) commentcount,  
(d.likecount * 1) likecount,  
(d.viewcount * 1) viewcount, 
d.publicurl,
d.lastupdated 
  from quantitycheck q
left join {{ref('hb_deck_details')}} d
on q.id = d.id
order by quantity desc 