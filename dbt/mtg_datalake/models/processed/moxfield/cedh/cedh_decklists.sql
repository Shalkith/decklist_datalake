{{ config(
    materialized = "view"
) }}

{{decklists('cedh')}}