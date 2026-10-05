{{ config(enabled=var('inject_failure', false), severity='error') }}

-- A controlled test used only by the failure drill. A returned row must make dbt fail.
select 'intentional AtmosPulse dbt failure injection' as failure

