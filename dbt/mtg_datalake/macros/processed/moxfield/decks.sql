{% macro decks(tablename) %}
    WITH decks_per_commander AS (
        SELECT
            CASE
                WHEN card_name LIKE 'A-%' THEN REPLACE(
                    card_name,
                    "A-",
                    ""
                )
                ELSE card_name
            END AS commander,
            COUNT(
                DISTINCT deck_id
            ) total_decks
        FROM
            {{ ref(tablename+'_board_commanders') }}
        GROUP BY
            1
        ORDER BY
            2 DESC
    ),
    stats AS (
        SELECT
            CASE
                WHEN hbc.card_name LIKE 'A-%' THEN REPLACE(
                    hbc.card_name,
                    "A-",
                    ""
                )
                ELSE hbc.card_name
            END AS commander,
            CASE
                WHEN hd.card_name LIKE 'A-%' THEN REPLACE(
                    hd.card_name,
                    'A-',
                    ''
                )
                ELSE hd.card_name
            END AS card_name,
            hd.type,
            SUM(
                hd.quantity
            ) quantity,
            MAX(
                d.total_decks
            ) AS total_decks,
            SUM(
                hd.quantity
            ) / MAX(d.total_decks) AS AVG,
            CEIL(SUM(hd.quantity) / MAX(d.total_decks)) AS ceilavg
        FROM
            {{ ref(tablename+'_decklists') }}
            hd
            LEFT JOIN {{ ref(tablename+'_board_commanders') }}
            hbc
            ON hd.deck_id = hbc.deck_id
            LEFT JOIN decks_per_commander d
            ON d.commander = hbc.card_name
        WHERE
            hbc.card_name IS NOT NULL
        GROUP BY
            1,
            2,
            3
    ),
    running_totals AS (
        SELECT
            *,
            SUM(ceilavg) over (
                PARTITION BY commander
                ORDER BY
                    AVG DESC,
                    card_name ASC
            ) AS running_total
        FROM
            stats
        ORDER BY
            1 ASC,
            4 DESC
    )
SELECT
    commander,
    card_name,
    ceilavg AS quantity,
    total_decks
FROM
    running_totals
WHERE
    running_total <= 100
ORDER BY
    commander ASC,
    TYPE ASC,
    ceilavg DESC
{% endmacro %}
