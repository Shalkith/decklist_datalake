{{ config(
    materialized = "view"
) }}

WITH commanders AS (

    SELECT * FROM {{ ref('cedh_board_commanders') }}
),
mainboard AS (
    SELECT * FROM {{ ref('cedh_board_mainboard') }}
),
companions AS (
    SELECT * FROM {{ ref('cedh_board_companions') }}
),
FINAL AS (
    SELECT
        *,
        'Commander' AS TYPE
    FROM
        commanders
    UNION ALL
    SELECT
        *,
        'Mainboard' AS TYPE
    FROM
        mainboard
    UNION ALL
    SELECT
        *,
        'Companion' AS TYPE
    FROM
        companions
),
quantity_validate AS (
    SELECT
        DISTINCT deck_id
    FROM
        FINAL d
    GROUP BY
        1
    HAVING
        SUM(quantity) = 100
)
SELECT
    *
FROM
    FINAL
WHERE
    deck_id IN (
        SELECT
            *
        FROM
            quantity_validate
    )
ORDER BY
    1,
    5 ASC,
    4 DESC
