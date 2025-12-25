{{
    config(
        materialized='incremental',
        unique_key=['deck_id','card_name']
    )
}}
-- depends_on: {{ ref("historicbrawl_decks_history") }}

{{board_mainboard('historicbrawl')}}