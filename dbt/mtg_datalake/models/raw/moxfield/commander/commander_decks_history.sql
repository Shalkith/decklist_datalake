{{
    config(
        materialized='incremental',
        unique_key='id',
    )
}}

select * from {{source('mtg_datalake', 'commander_decks')}}

