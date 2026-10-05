from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class WarehouseSettings:
    project_id: str
    location: str
    staging_dataset: str
    analytics_dataset: str
    monitoring_dataset: str

    @classmethod
    def from_env(cls) -> WarehouseSettings:
        project = os.getenv("ATMOSPULSE_BQ_PROJECT", "").strip()
        if not project:
            raise ValueError("ATMOSPULSE_BQ_PROJECT is required")
        return cls(
            project_id=project,
            location=os.getenv("ATMOSPULSE_BQ_LOCATION", "US"),
            staging_dataset=os.getenv("ATMOSPULSE_BQ_STAGING_DATASET", "atmospulse_staging"),
            analytics_dataset=os.getenv("ATMOSPULSE_BQ_ANALYTICS_DATASET", "atmospulse_analytics"),
            monitoring_dataset=os.getenv(
                "ATMOSPULSE_BQ_MONITORING_DATASET", "atmospulse_monitoring"
            ),
        )
