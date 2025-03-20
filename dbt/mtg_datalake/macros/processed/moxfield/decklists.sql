
{% macro decklists(tablename) %}

WITH 
latest_commanders AS (
    SELECT deck_id,max(lastupdated) lastupdated FROM {{ ref(tablename+'_board_commanders') }}
),
latest_mainboard AS (
    SELECT deck_id,max(lastupdated) lastupdated FROM {{ ref(tablename+'_board_mainboard') }}
),
latest_companions AS (
    SELECT deck_id,max(lastupdated) lastupdated FROM {{ ref(tablename+'_board_companions') }}
),
commanders AS (
    SELECT * FROM {{ ref(tablename+'_board_commanders') }}
    inner join latest_commanders lc
    on commanders.deck_id = lc.deck_id and commanders.lastupdated = lc.lastupdated
),
mainboard AS (
    SELECT * FROM {{ ref(tablename+'_board_mainboard') }}
    inner join latest_mainboard lm
    on mainboard.deck_id = lm.deck_id and mainboard.lastupdated = lm.lastupdated
),
companions AS (
    SELECT * FROM {{ ref(tablename+'_board_companions') }}
    inner join latest_companions lcc
    on companions.deck_id = lcc.deck_id and companions.lastupdated = lcc.lastupdated
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



{% endmacro %}