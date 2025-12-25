{% macro deck_build_progress(tablename) %}
    WITH prices AS (
        SELECT
            deck_id,
            SUM(cost) AS cost
        FROM
            (
                SELECT
                    hd.*,CASE
                        WHEN sp.min_usd * hd.quantity > 9999
                        OR sp.min_usd * hd.quantity < 0 THEN 0
                        ELSE sp.min_usd * hd.quantity
                    END AS cost
                FROM
                    {{ ref(tablename+'_decklists') }}
                    hd
                    LEFT JOIN {{ ref('scryfall_prices') }}
                    sp
                    ON sp.name = hd.card_name
            ) SUM
        GROUP BY
            1
    ),
    decks AS (
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
                WHEN NAME LIKE 'Forest' THEN 99
                WHEN NAME LIKE 'Swamp' THEN 99
                WHEN NAME LIKE 'Island' THEN 99
                WHEN NAME LIKE 'Plains' THEN 99
                WHEN NAME LIKE 'Mountain' THEN 99
                WHEN NAME LIKE 'Wastes' THEN 99
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
            DISTINCT deck_id,
            SUM(quantity) quantity
        FROM
            combined
        WHERE
            NAME IS NOT NULL
        GROUP BY
            1
        HAVING
            SUM(quantity) <= 100
    )
SELECT
    q.deck_id,
    d.commander_name,
    d.deck_name,
    d.card_count,
    q.quantity AS percent_complete,
    p.cost AS price,
    d.hubs,
    d.description,
    (
        d.commentcount * 1
    ) commentcount,
    (
        d.likecount * 1
    ) likecount,
    (
        d.viewcount * 1
    ) viewcount,
    d.publicurl,
    d.lastUpdatedAtUtc
FROM
    quantitycheck q
    LEFT JOIN {{ ref(tablename+'_deck_details') }}
    d
    ON q.deck_id = d.deck_id
    LEFT JOIN prices p
    ON p.deck_id = d.deck_id
ORDER BY
    quantity DESC
{% endmacro %}
