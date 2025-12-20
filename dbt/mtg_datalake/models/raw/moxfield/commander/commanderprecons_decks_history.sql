{{
    config(
        materialized='incremental',
        unique_key='id',
    )
}}

select * from {{source('mtg_datalake', 'commanderprecons_decks')}}

