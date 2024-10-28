
with
decks_per_commander as (
select case when card_name  like 'A-%' then replace(card_name,"A-","") else card_name  end as commander,
count(distinct deck_id) total_decks from {{ref('hb_board_commanders')}} 
group by 1 order by 2 desc 
),
stats as (
select 
case when hbc.card_name  like 'A-%' then replace(hbc.card_name,"A-","") else hbc.card_name  end as commander,
case when hd.card_name  like 'A-%' then replace(hd.card_name,'A-','') else hd.card_name  end as card_name,
hd.type,
sum(hd.quantity) quantity ,
max(d.total_decks) as total_decks,
sum(hd.quantity) / max(d.total_decks) as avg,
ceil(sum(hd.quantity) / max(d.total_decks)) as ceilavg
from  {{ref('hb_decklists')}} hd
left join {{ref('hb_board_commanders')}}  hbc 
	on hd.deck_id = hbc.deck_id  
LEFT join decks_per_commander d 
	on d.commander = hbc.card_name
where hbc.card_name is not null
group by 1,2,3
),
running_totals as (
select *,
SUM(ceilavg) OVER (PARTITION BY commander ORDER BY avg desc,card_name asc ) AS running_total
from stats
order by 1 asc,4 desc)
select commander,
card_name,
ceilavg as quantity,
total_decks
from running_totals
where running_total <= 100
order by commander asc, type asc, ceilavg desc