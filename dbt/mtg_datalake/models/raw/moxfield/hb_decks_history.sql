{{
    config(
        materialized='incremental',
        unique_key=['deck_id']
    )
}}

select * from {{ source('mtg_datalake','historicbrawl_decks') }}

