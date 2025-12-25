{{ config(
    materialized = "view"
) }}

{{decklists('budget_edh')}}