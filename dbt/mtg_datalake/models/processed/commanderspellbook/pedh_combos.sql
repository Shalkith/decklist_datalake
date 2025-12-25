WITH unnest1 AS (
    SELECT
        pcch.id,
        pcch.status,
        jt.card_id,
        jt.oracleid,
        jt.name,
        json_unquote(json_extract(pcch.combodata, '$.uses')) uses,
        json_unquote(json_extract(pcch.combodata, '$.requires')) requires,
        json_unquote(json_extract(pcch.combodata, '$.produces')) produces,
        json_unquote(json_extract(pcch.combodata, '$.of')) "of",
        json_unquote(json_extract(pcch.combodata, '$.includes')) includes,
        json_unquote(json_extract(pcch.combodata, '$.identity')) identity,
        json_unquote(json_extract(pcch.combodata, '$.manaNeeded')) manaNeeded,
        json_unquote(json_extract(pcch.combodata, '$.manaValueNeeded')) manaValueNeeded,
        json_unquote(json_extract(pcch.combodata,'$.otherPrerequisites')) otherPrerequisites,
        json_unquote(json_extract(pcch.combodata, '$.description')) description,
        json_unquote(json_extract(pcch.combodata, '$.notes')) notes,
        json_unquote(json_extract(pcch.combodata, '$.popularity')) popularity,
        json_unquote(json_extract(pcch.combodata, '$.spoiler')) spoiler,
        json_unquote(json_extract(pcch.combodata, '$.legalities')) legalities,
        json_unquote(json_extract(pcch.combodata, '$.prices')) prices
    FROM
        {{ref('pauper_commander_combos_history')}} pcch
        JOIN json_table(
            pcch.combodata,
            '$.uses[*]' COLUMNS (
                card_id INT path '$.card.id',
                NAME VARCHAR(255) path '$.card.name',
                oracleid VARCHAR(255) path '$.card.oracleId'
            )
        ) AS jt
    WHERE
        pcch.dbt_valid_to IS NULL
)
SELECT
    *
FROM
    unnest1
