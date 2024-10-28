
with 
decks as (
select * from {{ref('hb_decklists')}}
),
mycards as (select name, 
case 
when name like 'Snow-Covered%' then 99  
when name like 'Forest' then 99  
when name like 'Swamp' then 99  
when name like 'Island' then 99  
when name like 'Plains' then 99  
when name like 'Mountain' then 99  
when name like 'Wastes' then 99 
else quantity end as quantity 
from {{ref('mtga_collection')}} mc ),
combined as (
select d.*,m.name from decks d left join mycards m on d.card_name = m.name and d.quantity <= m.quantity
),
quantitycheck as (
select distinct deck_id, sum(quantity) quantity from combined 
where name is not NULL 
group by 1
having sum(quantity) <= 100
)
select q.deck_id,d.commander_name, d.deck_name,
d.card_count,
 q.quantity as percent_complete,
 d.hubs,
 d.description,
(d.commentcount * 1) commentcount,  
(d.likecount * 1) likecount,  
(d.viewcount * 1) viewcount, 
d.publicurl,
d.lastupdated 
  from quantitycheck q
left join {{ref('hb_deck_details')}} d
on q.deck_id = d.deck_id
order by quantity desc 