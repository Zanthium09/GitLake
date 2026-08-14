-- One row per repo_id, latest attributes by created_at -- same reasoning as
-- stg_actors: repo_name can change (renames, transfers), repo_id does not.
-- repo_owner/repo_short_name are split out here because GitHub's own
-- "owner/name" format is a single string in the source; splitting it is
-- cleaning, not business logic, so it belongs at this layer.

select
    repo_id,
    trim(repo_name)                              as repo_name,
    split_part(trim(repo_name), '/', 1)          as repo_owner,
    split_part(trim(repo_name), '/', 2)          as repo_short_name
from {{ source('raw', 'EVENTS') }}
where repo_id is not null
qualify row_number() over (
    partition by repo_id
    order by created_at desc
) = 1
