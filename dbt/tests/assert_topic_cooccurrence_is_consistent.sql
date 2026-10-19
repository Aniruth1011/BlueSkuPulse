-- A pair can never be more frequent than either of its topics or than all tagged posts.
select *
from {{ ref('mart_topic_cooccurrence') }}
where topic_a >= topic_b
   or pair_posts > topic_a_posts
   or pair_posts > topic_b_posts
   or topic_a_posts > tagged_posts
   or topic_b_posts > tagged_posts
   or lift <= 0
   or (unique_authors < 2 and passes_spam_guardrail)
