
with
decks_per_commander as (
select case when card  like 'A-%' then replace(card,"A-","") else card  end as commander,
count(distinct id) total_decks from {{ref('hb_board_commanders')}} 
group by 1 order by 2 desc 
),
stats as (
select 
case when hbc.card  like 'A-%' then replace(hbc.card,"A-","") else hbc.card  end as commander,
case when hd.card  like 'A-%' then replace(hd.card,'A-','') else hd.card  end as card,
hd.type,
sum(hd.quantity) quantity ,
max(d.total_decks) as total_decks,
sum(hd.quantity) / max(d.total_decks) as avg,
ceil(sum(hd.quantity) / max(d.total_decks)) as ceilavg
from  {{ref('hb_decklists')}} hd
left join {{ref('hb_board_commanders')}}  hbc 
	on hd.id = hbc.id  
LEFT join decks_per_commander d 
	on d.commander = hbc.card
where hbc.card is not null
group by 1,2,3
),
running_totals as (
select *,
SUM(ceilavg) OVER (PARTITION BY commander ORDER BY avg desc,card asc ) AS running_total
from stats
order by 1 asc,4 desc)
select commander,
card,
ceilavg as quantity,
total_decks
from running_totals
where running_total <= 100
order by commander asc, type asc, ceilavg desc