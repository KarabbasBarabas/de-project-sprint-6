drop table if exists STV2023111337__STAGING.group_log;
create table STV2023111337__STAGING.group_log(
group_id bigint primary key,
user_id bigint,
user_id_from bigint,
"event" varchar, 
"datetime"  timestamp
)
order by user_id, user_id_from, group_id
segmented by hash(group_id) all nodes;
