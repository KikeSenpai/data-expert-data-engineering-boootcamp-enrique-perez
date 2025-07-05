create table if not exists host_activity_reduced (
    host text,
    hit_array bigint[],
    unique_visitors bigint[],
    "month" date,
    date_partition date,
    primary key (host, date_partition, "month")
);
