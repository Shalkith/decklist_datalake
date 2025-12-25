{{ config(
    materialized = "view"
) }}

{{decklists('historicbrawl')}}