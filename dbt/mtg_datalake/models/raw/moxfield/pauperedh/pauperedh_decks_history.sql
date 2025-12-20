{{
    config(
        materialized='incremental',
        unique_key='id',
        alias='pauperedh_decks_history'
    )
}}

select * from {{source('mtg_datalake', 'pauperedh_decks')}}

