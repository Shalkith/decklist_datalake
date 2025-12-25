{% macro deck_details(tablename) %}

{{
    config(
        materialized='incremental',
        unique_key=['deck_id']
    )
}}


    WITH decks AS (
        SELECT
            id,
            name AS deck_name,
            description,
            publicurl,
            likecount,
            viewcount,
            commentcount,
            colors,
            lastUpdatedAtUtc,
            createdAtUtc,
            hubs 
        FROM
            {{ ref(tablename+'_decks_history') }}
            hdh
{% if is_incremental() %}
        WHERE lastUpdatedAtUtc > (SELECT MAX(lastUpdatedAtUtc) FROM {{this}})
 {% endif %}
    ),
    commanders AS (
        SELECT
            deck_id,
            group_concat(
                card_name
                ORDER BY
                    card_name ASC separator '[and]'
            ) AS card_name
        FROM
            {{ ref(tablename+'_board_commanders') }}
        GROUP BY
            deck_id
    ),
    distinct_cards AS (
        SELECT
            deck_id,
            COUNT(
                DISTINCT card_name
            ) COUNT
        FROM
            {{ ref(tablename+'_decklists') }}
        GROUP BY
            1
    )
SELECT
    d.id AS deck_id,
    C.card_name AS commander_name,
    d.deck_name,
    description,
    dc.count + 0 AS card_count,
    d.colors,
    d.publicurl,
    d.likecount,
    d.viewcount,
    d.commentcount,
    d.lastUpdatedAtUtc,
    d.createdAtUtc,
    d.hubs
FROM
    decks d
    LEFT JOIN commanders C
    ON d.id = C.deck_id
    LEFT JOIN distinct_cards dc
    ON C.deck_id = dc.deck_id
{% endmacro %}
