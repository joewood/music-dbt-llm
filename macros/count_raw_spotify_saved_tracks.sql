{% macro count_raw_spotify_saved_tracks() %}
  {% set relation = adapter.get_relation(database=target.database, schema='raw', identifier='spotify_saved_tracks') %}

  {% if relation is none %}
    {% do log('Relation raw.spotify_saved_tracks not found.', info=True) %}
    {{ return(0) }}
  {% endif %}

  {% set result = run_query('select count(*) as row_count from raw.spotify_saved_tracks') %}
  {% if execute and result is not none and (result.rows | length) > 0 %}
    {% set row_count = result.rows[0][0] %}
    {% do log('raw.spotify_saved_tracks row_count=' ~ row_count, info=True) %}
    {{ return(row_count) }}
  {% endif %}

  {{ return(0) }}
{% endmacro %}
