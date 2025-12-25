SELECT
    NAME,
    SUM(quantity) AS quantity
FROM
    {{ source(
        'mtg_datalake','aetherhub-export-helvault'
    ) }}
group by 1 order by 2 desc
