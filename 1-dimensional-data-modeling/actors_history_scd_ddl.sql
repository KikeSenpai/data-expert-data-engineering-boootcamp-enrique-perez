create table if not exists actors_history_scd (
    id UUID default gen_random_uuid(),
    actor text,
    actor_id text,
    quality_class text,
    is_active boolean,
    start_date date,
    end_date date,
    primary key (id)
);
