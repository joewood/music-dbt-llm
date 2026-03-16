{% macro export_musicbrainz_enrichment_queue(output_path='exports/musicbrainz/enrichment_queue.csv', max_unenriched=1000) %}
  {% if execute %}
    {% set normalized_path = output_path | replace('\\', '/') | replace("'", "''") %}
    {% set queue_relation = ref('stg_musicbrainz_enrichment_queue') %}
    {% set sql %}
      copy (
        select
          track_id,
          isrc,
          isrc_queue_rank,
          isrc_track_rank
        from {{ queue_relation }}
        where isrc_queue_rank <= {{ max_unenriched | int }}
        order by isrc, track_id
      )
      to '{{ normalized_path }}'
      (header, delimiter ',');
    {% endset %}
    {% do run_query(sql) %}
    {{ log('Exported MusicBrainz enrichment queue to ' ~ output_path, info=True) }}
  {% endif %}
{% endmacro %}
