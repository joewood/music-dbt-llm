{% macro _musicbrainz_csv_exists(csv_path) %}
  {% if not execute %}
    {{ return(false) }}
  {% endif %}

  {% set normalized_path = csv_path | replace('\\', '/') | replace("'", "''") %}
  {% set result = run_query("select count(*) from glob('" ~ normalized_path ~ "')") %}
  {% if result is not none and (result.rows | length) > 0 and (result.rows[0][0] | int) > 0 %}
    {{ return(true) }}
  {% endif %}
  {{ return(false) }}
{% endmacro %}


{% macro load_musicbrainz_enrichment_results(input_dir='exports/musicbrainz/results') %}
  {% if execute %}
    {% set normalized_input_dir = input_dir | replace('\\', '/') | replace("'", "''") %}

    {# Ensure target raw tables exist before loading CSV outputs. #}
    {% do init_raw_spotify_tables() %}

    {% set candidates_csv = normalized_input_dir ~ '/musicbrainz_isrc_candidates.csv' %}
    {% if _musicbrainz_csv_exists(candidates_csv) %}
      {% do run_query("delete from raw.musicbrainz_isrc_candidates where track_id in (select distinct track_id from read_csv_auto('" ~ candidates_csv ~ "', header=true))") %}
      {% do run_query("insert into raw.musicbrainz_isrc_candidates select track_id, upper(trim(isrc)) as isrc, recording_mbid, cast(match_rank as integer) as match_rank, matched_at, isrc_response_json from read_csv_auto('" ~ candidates_csv ~ "', header=true)") %}
      {{ log('Loaded musicbrainz_isrc_candidates.csv into raw.musicbrainz_isrc_candidates', info=True) }}
    {% else %}
      {{ log('Skipping missing file: ' ~ candidates_csv, info=True) }}
    {% endif %}

    {% set recording_payloads_csv = normalized_input_dir ~ '/musicbrainz_recording_payloads.csv' %}
    {% if _musicbrainz_csv_exists(recording_payloads_csv) %}
      {% do run_query("delete from raw.musicbrainz_recording_payloads where recording_mbid in (select distinct recording_mbid from read_csv_auto('" ~ recording_payloads_csv ~ "', header=true))") %}
      {% do run_query("insert into raw.musicbrainz_recording_payloads select recording_mbid, payload_json, last_seen_at from read_csv_auto('" ~ recording_payloads_csv ~ "', header=true)") %}
      {{ log('Loaded musicbrainz_recording_payloads.csv into raw.musicbrainz_recording_payloads', info=True) }}
    {% else %}
      {{ log('Skipping missing file: ' ~ recording_payloads_csv, info=True) }}
    {% endif %}

    {% set work_payloads_csv = normalized_input_dir ~ '/musicbrainz_work_payloads.csv' %}
    {% if _musicbrainz_csv_exists(work_payloads_csv) %}
      {% do run_query("delete from raw.musicbrainz_work_payloads where work_mbid in (select distinct work_mbid from read_csv_auto('" ~ work_payloads_csv ~ "', header=true))") %}
      {% do run_query("insert into raw.musicbrainz_work_payloads select work_mbid, payload_json, last_seen_at from read_csv_auto('" ~ work_payloads_csv ~ "', header=true)") %}
      {{ log('Loaded musicbrainz_work_payloads.csv into raw.musicbrainz_work_payloads', info=True) }}
    {% else %}
      {{ log('Skipping missing file: ' ~ work_payloads_csv, info=True) }}
    {% endif %}

    {% set failed_isrcs_csv = normalized_input_dir ~ '/musicbrainz_isrc_failures.csv' %}
    {% if _musicbrainz_csv_exists(failed_isrcs_csv) %}
      {% do run_query("update raw.musicbrainz_isrc_failures as tgt set error_status_code = src.error_status_code, error_message = src.error_message, last_failed_at = src.last_failed_at, error_404_count = coalesce(tgt.error_404_count, 0) + case when src.error_status_code = '404' then 1 else 0 end from (select upper(trim(cast(isrc as varchar))) as isrc, nullif(trim(cast(error_status_code as varchar)), '') as error_status_code, cast(error_message as varchar) as error_message, cast(last_failed_at as varchar) as last_failed_at from read_csv_auto('" ~ failed_isrcs_csv ~ "', header=true)) as src where upper(trim(tgt.isrc)) = src.isrc") %}
      {% do run_query("insert into raw.musicbrainz_isrc_failures (isrc, error_status_code, error_message, last_failed_at, error_404_count) select src.isrc, src.error_status_code, src.error_message, src.last_failed_at, case when src.error_status_code = '404' then 1 else 0 end as error_404_count from (select upper(trim(cast(isrc as varchar))) as isrc, nullif(trim(cast(error_status_code as varchar)), '') as error_status_code, cast(error_message as varchar) as error_message, cast(last_failed_at as varchar) as last_failed_at from read_csv_auto('" ~ failed_isrcs_csv ~ "', header=true)) as src where not exists (select 1 from raw.musicbrainz_isrc_failures tgt where upper(trim(tgt.isrc)) = src.isrc)") %}
      {{ log('Loaded musicbrainz_isrc_failures.csv into raw.musicbrainz_isrc_failures', info=True) }}
    {% else %}
      {{ log('Skipping missing file: ' ~ failed_isrcs_csv, info=True) }}
    {% endif %}
  {% endif %}
{% endmacro %}


{% macro run_musicbrainz_enrichment_ops(output_path='exports/musicbrainz/enrichment_queue.csv', max_unenriched=1000, input_dir='exports/musicbrainz/results', do_export=true, do_load=true) %}
  {% if do_export %}
    {% do export_musicbrainz_enrichment_queue(output_path=output_path, max_unenriched=max_unenriched) %}
  {% endif %}

  {% if do_load %}
    {% do load_musicbrainz_enrichment_results(input_dir=input_dir) %}
  {% endif %}
{% endmacro %}
