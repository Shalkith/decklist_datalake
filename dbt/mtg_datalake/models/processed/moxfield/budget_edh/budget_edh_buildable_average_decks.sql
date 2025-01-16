

WITH decks AS (
    SELECT
        *
    FROM
        {{ ref('budget_edh_average_decks') }}
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
)
,
quantitycheck AS (
    SELECT
        DISTINCT commander
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
    *,
    CONCAT(quantity,' ',card) as copy_paste
FROM
    combined
WHERE
    commander IN (
        SELECT
            *
        FROM
            quantitycheck
    )
ORDER BY
    1,
    case when commander = card then 'a' else 'b' end asc,
    3 deSC