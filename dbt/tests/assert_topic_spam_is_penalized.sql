select *
from {{ ref('mart_topic_velocity') }}
where (unique_authors < 2 or author_concentration > 0.5)
  and (ranking_score <> 0 or passes_spam_guardrail)
