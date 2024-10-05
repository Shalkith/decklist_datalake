with 
commanders as (
    select * from {{ ref('hb_board_commanders') }}
),
mainboard as (
    select * from {{ ref('hb_board_mainboard') }}
),
companions as (
    select * from {{ ref('hb_board_companions') }}
),
final as (
    select *,'Commander' as type from commanders
    union all
    select *,'Mainboard' as type from mainboard
    union all
    select *,'Companion' as type from companions
)
select * from final

