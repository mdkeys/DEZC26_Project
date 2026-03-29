**About** 

dbt Cloud connects to BigQuery and runs the transformation layer: A staging model cleans and casts the raw data, an intermediate model enriches it with derived fields like resolution time and complaint flags, and three mart models aggregate the data into analysis-ready tables in nyc_311_prod.

**dbt Lineage**

![dbt Lineage Graph](images/dbt_lineage.png)

The dbt project follows a three-layer architecture:
- **Staging** (`stg_311_requests`) — type casting, deduplication, borough standardization
- **Intermediate** (`int_requests_enriched`) — derives `resolution_days`, `is_resolved`, `resolved_within_30_days`, `time_of_day`, `complaint_category`
- **Marts** — 3 focused tables for the 4 dashboard tiles, all filtered to housing complaint types.


**dbt files** 
- **dbt_project.yml** is the main config file. It sets the project name (nyc_311), tells dbt where to find models/macros/tests, and defines the materialization strategy for each layer.
- **macros/housing_complaint_types.sql** is a reusable macro that returns the list of 11 housing complaint types. 
- **models/sources.yml** registers the BigQuery external table (external_311_requests) as a dbt source so models can reference it with {{ source() }} instead of hardcoded table names.
- **models/staging/**
  - **stg_311_requests.sql** is the first transformation layer. Deduplicates rows by unique_key, casts all columns to proper types (Socrata returns everything as strings), standardizes columns, uses regex to safely extract numeric values from messy council_district and police_precinct fields, and filters out rows with null keys, null dates, or invalid boroughs.
  - **stg_311_requests.yml** defines column descriptions and data quality tests for the staging model — including unique and not_null on unique_key, and accepted_values tests on status and borough.
- **models/intermediate/**
  - **int_requests_enriched.sql** is the enrichment layer that derives five new fields from the staging columns: 'complaint_category', 'resolution_days', 'is_resolved', 'time_of_day', and 'day_of_week'. 
- **models/marts/**
  - **marts.yml** defines column descriptions and data quality tests for all 3 mart tables, including not_null, unique, and accepted_values tests on key columns.
  - **mart_heat_hotwater.sql** aggregates `HEAT/HOT WATER` complaints  by borough, year, month, and season (heat season Oct–May, vs non-heat season). Outputs complaint volume, resolution rate, and avg resolution days per group. Powers the heat & hot water trend tile in Looker Studio.
  - **mart_housing_response_times.sql** aggregates all 11 housing complaint types by borough, complaint type, and year. Outputs total complaints, resolved complaints, avg resolution days, median resolution days, and pct resolved within 30 days. Powers the response times tile in Looker Studio.
  - **mart_complaints_geo.sql** filters to housing complaints with valid NYC coordinates (bounding box check). Outputs one row per complaint with latitude, longitude, complaint type, borough, community board, and resolution details. Powers the geographic heatmap tile in Looker Studio.


### Resources from dbt:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [dbt community](https://getdbt.com/community) to learn from other analytics engineers
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
