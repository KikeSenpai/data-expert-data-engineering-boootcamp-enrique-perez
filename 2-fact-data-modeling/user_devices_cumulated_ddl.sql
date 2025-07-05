create type browser_datelist as (
    browser_type text,
    datelist date[]
);


create table if not exists user_devices_cumulated (
    user_id numeric not null,
    device_activity_datelist browser_datelist[],
    last_updated_at date,
    primary key (user_id)
);
