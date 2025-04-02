{{
    config(
        materialized='incremental',
        unique_key=['deck_id']
    )
}}

{{deck_details('cedh')}}
