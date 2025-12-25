{{ config(
    materialized = "view"
) }}


WITH combo_pieces_owned AS (
    SELECT
        cc.id,
        COUNT(
            DISTINCT mc.name
        ) AS owned
    FROM
        {{ref('hb_combos')}} cc
        INNER JOIN {{ref('mtga_collection')}} mc
        ON CAST(
            mc.name AS CHAR
        ) = CAST(
            cc.name AS CHAR
        )
    GROUP BY
        1
),
combos AS (
    SELECT
        id,
        identity,
        COUNT(
            DISTINCT NAME
        ) needed
    FROM
        {{ref('hb_combos')}} cc
    GROUP BY
        1,
        2
    ORDER BY
        3 DESC
),
usable_combos AS (
    SELECT
        DISTINCT C.id
    FROM
        combos C
        LEFT JOIN combo_pieces_owned co
        ON C.id = co.id
    WHERE
        C.needed = co.owned
)
SELECT
    cc.*
FROM
    {{ref('hb_combos')}} cc
    INNER JOIN usable_combos u
    ON cc.id = u.id
