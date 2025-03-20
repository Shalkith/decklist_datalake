{% macro deck_details(tablename) %}
    WITH decks AS (
        SELECT
            id,
            json_unquote(json_extract(deckdata, '$.name')) AS deck_name,
            json_unquote(json_extract(deckdata, '$.description')) AS description,
            json_unquote(json_extract(deckdata, '$.publicUrl')) AS publicurl,
            json_unquote(json_extract(deckdata, '$.likeCount')) AS likecount,
            json_unquote(json_extract(deckdata, '$.viewCount')) AS viewcount,
            json_unquote(json_extract(deckdata, '$.commentCount')) AS commentcount,
            json_unquote(json_extract(deckdata, '$.colors')) AS colors,
            json_unquote(json_extract(deckdata, '$.lastUpdatedAtUtc')) AS lastupdated,
            json_unquote(json_extract(deckdata, '$.createdAtUtc')) AS createdat,
            json_unquote(json_extract(deckdata, '$.hubs')) AS hubs
        FROM
            {{ ref(tablename+'_decks_history') }}
            hdh
        WHERE
            dbt_valid_to IS NULL
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
    d.lastupdated,
    d.createdat,
    hubs
FROM
    decks d
    LEFT JOIN commanders C
    ON d.id = C.deck_id
    LEFT JOIN distinct_cards dc
    ON C.deck_id = dc.deck_id
{% endmacro %}
