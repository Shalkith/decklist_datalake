{{
    config(
        materialized='incremental',
        unique_key='id',
        alias='historicbrawl_decks_history'
    )
}}

select * from {{ source('mtg_datalake','historicbrawl_decks') }}

