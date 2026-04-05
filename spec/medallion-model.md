The rules of medallion model are as following:

In this ELT project, Bronze includes both raw landing tables and dbt staging models
that unpack/normalize extracted payloads.

## Bronze Staging Models

* Should only extract data from the source yaml files
* Should never join and enrich
* Should make sure all fields adhere to standard conventions
* Should make sure dates and timestamps are in standard format
* Should clearly mark the primary key as `id`
* No other models outside of staging should query from sources
* JSON unpacking/flattening from extracted payloads belongs in this layer

## Silver Enrichment Models

* Should perform enrichment and joining
* Should clearly define which models are dimensional and which are facts by using separate directories
* Should never query from `source` tables
* Should use materialized models for dimensions when needed
* Should use the `int_` prefix

## Gold Mart Models

* Should clearly define between dimensions and fact models using `dim_` and `fct_` prefixes
* Should source data primarily from staging
* Should not perform enrichment
* Should perform use-case projections for output
* Should use materialized models when needed using the merge method on `id`