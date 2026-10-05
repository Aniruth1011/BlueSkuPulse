select *
from {{ ref('mart_pipeline_health') }}
where unexplained_delta <> 0 or reconciliation_delta_ratio <> 0
