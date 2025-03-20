{% macro top_cards_sql(tablename) %}

SELECT
    hd.card_name,
    s1.oracle_id,
    s1.type_line,
    COALESCE(
        s1.image_url,
        s3.image_url
    ) card_front,
    COALESCE(
        s2.image_url,
        s4.image_url
    ) card_back,
    COUNT(
        DISTINCT deck_id
    )
FROM
    {{ ref(tablename) }} hd
    LEFT JOIN mtg_datalake.scryfall_images s1
    ON substring_index(
        hd.card_name,
        '[and]',
        1
    ) = s1.card_name
    LEFT JOIN mtg_datalake.scryfall_images s2
    ON substring_index(
        hd.card_name,
        '[and]',
        -1
    ) = s2.card_name
    AND s2.card_name NOT LIKE s1.card_name
    LEFT JOIN mtg_datalake.scryfall_images s3
    ON substring_index(
        hd.card_name,
        ' // ',
        1
    ) = s3.card_name
    LEFT JOIN mtg_datalake.scryfall_images s4
    ON substring_index(
        hd.card_name,
        ' // ',
        -1
    ) = s4.card_name
    AND s4.card_name NOT LIKE s3.card_name
WHERE
lower(s1.type_line) not like '%land%' and 
    hd.card_name NOT IN (
        'Plains',
        'Island',
        'Swamp',
        'Mountain',
        'Forest',
        'Snow-Covered Plains',
        'Snow-Covered Island',
        'Snow-Covered Swamp',
        'Snow-Covered Mountain',
        'Snow-Covered Forest'
    )
GROUP BY
    1,
    2,
    3,
    4,
    5
ORDER BY
    6 DESC
LIMIT
    200
{% endmacro %}