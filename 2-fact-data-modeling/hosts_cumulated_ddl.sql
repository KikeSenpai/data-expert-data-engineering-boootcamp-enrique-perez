create table if not exists hosts_cumulated (
    host text not null,
    activity_datelist date[],
    last_updated_at date,
    primary key (host)
);
