{{
    config(
        materialized='incremental',
        unique_key=['deck_id','card_name']
    )
}}

{{board_companions('budget_edh')}}