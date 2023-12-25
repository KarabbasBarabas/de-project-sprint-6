drop table if exists STV2023111337__DWH.s_auth_history;
create table STV2023111337__DWH.s_auth_history (
hk_l_user_group_activity bigint not null constraint s_auth_history_hk_l_user_group_activity_fkey references STV2023111337__DWH.l_user_group_activity (hk_l_user_group_activity),
user_id_from bigint,
"event" varchar,
event_dt timestamp,
load_dt DATETIME,
load_src VARCHAR(20)
)
order by hk_l_user_group_activity
SEGMENTED by hash(hk_l_user_group_activity) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);


insert into STV2023111337__DWH.s_auth_history (
    hk_l_user_group_activity,
    user_id_from,
    "event",
    event_dt,
    load_dt,
    load_src
)
select
    luga.hk_l_user_group_activity,
    gl.user_id_from,
    gl."event",
    gl."datetime" as event_dt,
    now() as load_dt,
    's3' as load_src
from STV2023111337__STAGING.group_log as gl
left join STV2023111337__DWH.h_groups as hg on gl.group_id = hg.group_id
left join STV2023111337__DWH.h_users as hu on gl.user_id = hu.user_id
left join STV2023111337__DWH.l_user_group_activity as luga on hg.hk_group_id = luga.hk_group_id and hu.hk_user_id = luga.hk_user_id;
