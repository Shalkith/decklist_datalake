{% snapshot hb_decks_history %}
{{
    config(
      unique_key='id',
      strategy='check',
      check_cols='all',
      invalidate_hard_deletes=True,
      target_database='mtg_datalake',
      target_schema='mtg_datalake',
      alias='historicbrawl_decks_history'
    )
}}

select * from {{source('mtg_datalake', 'historicbrawl_decks')}}

{% endsnapshot %}