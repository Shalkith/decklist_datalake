
with 
prices as (
select deck_id, sum(cost) as cost from (
select hd.*,case when sp.min_usd*hd.quantity > 9999 or sp.min_usd*hd.quantity < 0 then 0 else sp.min_usd*hd.quantity end as cost
from {{ref('budget_edh_decklists')}}  hd 
left join  {{ref('scryfall_prices')}} sp on sp.name = hd.card_name 
) sum group by 1 
),
decks as (
select * from {{ref('budget_edh_decklists')}}
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
 p.cost as price ,
 d.hubs,
 d.description,
(d.commentcount * 1) commentcount,  
(d.likecount * 1) likecount,  
(d.viewcount * 1) viewcount, 
d.publicurl,
d.lastupdated 
  from quantitycheck q
left join {{ref('budget_edh_deck_details')}} d
on q.deck_id = d.deck_id
left join prices p on p.deck_id = d.deck_id
order by quantity desc 