{{
    config(
        materialized='incremental',
        unique_key='id',
    )
}}

select * from {{source('mtg_datalake', 'cedh_decks')}}

