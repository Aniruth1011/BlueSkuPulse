with ranked as (
    select
        event_id,
        source_version,
        source_host,
        cast(cursor as bigint) as cursor,
        event_kind,
        operation,
        did,
        collection,
        rkey,
        at_uri,
        cid,
        cast(event_time as timestamp) as event_time,
        cast(ingested_at as timestamp) as ingested_at,
        payload_json,
        cast(schema_version as integer) as schema_version,
        cast(event_date as date) as event_date,
        row_number() over (
            partition by event_id order by ingested_at desc, cursor desc
        ) as event_rank
    from {{ source('silver', 'silver_events') }}
)
select
    event_id, source_version, source_host, cursor, event_kind, operation,
    did, collection, rkey, at_uri, cid, event_time, ingested_at,
    payload_json, schema_version, event_date
from ranked
where event_rank = 1
