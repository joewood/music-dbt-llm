{% macro clear_raw_spotify_saved_tracks() %}
  {% set relation = adapter.get_relation(database=target.database, schema='raw', identifier='spotify_saved_tracks') %}

  {% if relation is none %}
    {% do log('Relation raw.spotify_saved_tracks not found; nothing to delete.', info=True) %}
    {{ return('not_found') }}
  {% endif %}

  {% do run_query('delete from raw.spotify_saved_tracks') %}
  {% do run_query('commit') %}
  {% do log('Deleted all rows from raw.spotify_saved_tracks.', info=True) %}
  {{ return('ok') }}
{% endmacro %}
