

WITH decks AS (
    SELECT
        *
    FROM
        {{ ref('pedh_decklists') }}
),
mycards AS (
    SELECT
        NAME,
        CASE
            WHEN NAME LIKE 'Snow-Covered%' THEN 99
            ELSE quantity
        END AS quantity
    FROM
        {{ ref('mtga_collection') }}
        mc
),
combined AS (
    SELECT
        d.*,
        m.name
    FROM
        decks d
        LEFT JOIN mycards m
        ON d.card = m.name
        AND d.quantity <= m.quantity
),
quantitycheck AS (
    SELECT
        DISTINCT id
    FROM
        combined
    WHERE
        NAME IS NOT NULL
    GROUP BY
        1
    HAVING
        SUM(quantity) = 100
)
SELECT
    *
FROM
    combined
WHERE
    id IN (
        SELECT
            *
        FROM
            quantitycheck
    )
ORDER BY
    1,
    5 ASC,
    4 DESC
