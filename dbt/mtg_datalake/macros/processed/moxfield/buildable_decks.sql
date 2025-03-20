
{% macro buildable_decks(tablename) %}


WITH decks AS (
    SELECT
        *
    FROM
        {{ ref(tablename+'_decklists') }}
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
        ON d.card_name = m.name
        AND d.quantity <= m.quantity
),
quantitycheck AS (
    SELECT
        DISTINCT deck_id
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
    deck_id IN (
        SELECT
            *
        FROM
            quantitycheck
    )
ORDER BY
    1,
    5 ASC,
    4 DESC

{% endmacro %}