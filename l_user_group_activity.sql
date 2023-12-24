drop table if exists STV2023111337__DWH.l_user_group_activity cascade;
create table STV2023111337__DWH.l_user_group_activity (
hk_l_user_group_activity bigint primary key,
hk_user_id bigint constraint l_user_group_activity_hk_user_id_fkey references STV2023111337__DWH.h_users(hk_user_id),
hk_group_id bigint constraint l_user_group_activity_hk_group_id_fkey references STV2023111337__DWH.h_groups(hk_group_id),
load_dt DATETIME,
load_src VARCHAR(20)
) 
order by hk_user_id, hk_group_id, hk_l_user_group_activity
segmented by hash(hk_l_user_group_activity) all nodes
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2); 


insert into STV2023111337__DWH.l_user_group_activity(
    hk_l_user_group_activity,
    hk_user_id,
    hk_group_id,
    load_dt,
    load_src
)
select distinct
    hash(hu.user_id, hg.group_id) as hk_l_user_group_activity,
    hu.hk_user_id,
    hg.hk_group_id,
    now() as load_dt,
    's3' as load_src
from STV2023111337__STAGING.group_log gl
left join STV2023111337__DWH.h_users hu on hu.user_id = gl.user_id 
left join STV2023111337__DWH.h_groups hg on hg.group_id = gl.group_id 
where hash(hu.user_id, hg.group_id) not in (
    select 
        hash(g.hk_l_user_group_activity)
    from STV2023111337__DWH.l_user_group_activity g
    );