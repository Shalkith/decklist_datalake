{% snapshot cedh_decks_history %}
{{
    config(
      unique_key='id',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True,
      target_database='mtg_datalake',
      target_schema='mtg_datalake'
    )
}}

select * from {{source('mtg_datalake', 'cedh_decks')}}

{% endsnapshot %}