-- One row per actor_id. GitHub logins can change (renames), so this keeps
-- each actor's attributes as of their most recent event rather than an
-- arbitrary row -- QUALIFY + ROW_NUMBER picks the latest by created_at.
-- First/last-seen aggregation is business logic, not cleaning, so it lives
-- in int_actor_first_last_seen, not here.

select
    actor_id,
    trim(actor_login)          as actor_login,
    trim(actor_display_login)  as actor_display_login
from {{ source('raw', 'EVENTS') }}
where actor_id is not null
qualify row_number() over (
    partition by actor_id
    order by created_at desc
) = 1
