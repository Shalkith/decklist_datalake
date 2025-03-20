{% macro top_commanders(tablename) %}
SELECT
    commander_name,
    s1.oracle_id,
    COALESCE(
        s1.image_url,
        s3.image_url
    ),
    COALESCE(
        s2.image_url,
        s4.image_url
    ),
    COUNT(
        DISTINCT deck_id
    )
FROM
    {{ ref(tablename) }} 
    LEFT JOIN mtg_datalake.scryfall_images s1
    ON substring_index(
        commander_name,
        '[and]',
        1
    ) = s1.card_name
    LEFT JOIN mtg_datalake.scryfall_images s2
    ON substring_index(
        commander_name,
        '[and]',
        -1
    ) = s2.card_name
    AND s2.card_name NOT LIKE s1.card_name
    LEFT JOIN mtg_datalake.scryfall_images s3
    ON substring_index(
        commander_name,
        ' // ',
        1
    ) = s3.card_name
    LEFT JOIN mtg_datalake.scryfall_images s4
    ON substring_index(
        commander_name,
        ' // ',
        -1
    ) = s4.card_name
    AND s4.card_name NOT LIKE s3.card_name
WHERE
    commander_name IS NOT NULL
GROUP BY
    1,
    2,
    3,
    4
ORDER BY
    5 DESC
{% endmacro %}
