{{
    config(
        materialized='view'
    )
}}

with source as (
    select * from {{ source('nyc_311_raw', 'external_311_requests') }}
),

deduplicated as (
    select *,
        row_number() over (
            partition by unique_key
            order by created_date desc
        ) as row_num
    from source
),

renamed as (
    select
        -- identifiers
        unique_key,

        -- timestamps
        cast(created_date as timestamp)  as created_date,
        cast(closed_date  as timestamp)  as closed_date,
        cast(due_date     as timestamp)  as due_date,

        -- complaint details
        complaint_type,
        descriptor,
        location_type,
        lower(status)                    as status,

        -- agency
        agency,

        -- geography
        upper(trim(borough))             as borough,
        incident_zip,
        community_board,
        safe_cast(
            regexp_extract(council_district, r'(\d+)') as int64
        )                                as council_district,
        safe_cast(
            regexp_extract(police_precinct, r'(\d+)') as int64
        )                                as police_precinct,
        city,
        cast(latitude  as float64)       as latitude,
        cast(longitude as float64)       as longitude,

        -- partition columns
        cast(year  as int64)             as year,
        cast(month as int64)             as month

    from deduplicated
    where
        row_num = 1
        and unique_key   is not null
        and created_date is not null
        and upper(trim(borough)) in (
            'MANHATTAN', 'BROOKLYN', 'QUEENS', 'BRONX', 'STATEN ISLAND'
        )
)

select * from renamed