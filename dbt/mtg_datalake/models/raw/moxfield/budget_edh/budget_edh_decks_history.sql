
{{
    config(
        materialized='incremental',
        unique_key='id',
    )
}}

select * from {{source('mtg_datalake', 'budget_edh_decks')}}

