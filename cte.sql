-- Шаг 7.1. Подготовить CTE user_group_messages
with user_group_messages as (
    select 
        hg.hk_group_id, 
        count(distinct hu.hk_user_id) as cnt_users_in_group_with_messages
    from STV2023111337__DWH.h_groups hg 
    inner join STV2023111337__DWH.l_groups_dialogs gd on hg.hk_group_id = gd.hk_group_id 
    inner join STV2023111337__DWH.l_user_message um on um.hk_message_id = gd.hk_message_id 
    inner join STV2023111337__DWH.h_users hu on hu.hk_user_id = um.hk_user_id 
    group by hg.hk_group_id
)

select hk_group_id,
            cnt_users_in_group_with_messages
from user_group_messages
order by cnt_users_in_group_with_messages
limit 10
; 

-- Шаг 7.2. Подготовить CTE user_group_log
with user_group_log as (
    select
        hg.hk_group_id,
        count(distinct luga.hk_user_id) as cnt_added_users
    from (
    select 
        hg_t.hk_group_id 
    from STV2023111337__DWH.h_groups hg_t
    order by hg_t.registration_dt asc
    limit 10
    ) hg
    inner join STV2023111337__DWH.l_user_group_activity luga on luga.hk_group_id = hg.hk_group_id
    inner join STV2023111337__DWH.s_auth_history ah on ah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
    left join STV2023111337__DWH.l_user_message um on um.hk_user_id = luga.hk_user_id 
    where ah.event = 'add'
        and um.hk_user_id is null
    group by hg.hk_group_id    
)

select 
    hk_group_id,
    cnt_added_users
from user_group_log
order by cnt_added_users
limit 10
; 


-- Шаг 7.3. Написать запрос и ответить на вопрос бизнеса
with user_group_messages as (
    select 
        hg.hk_group_id, 
        count(distinct hu.hk_user_id) as cnt_users_in_group_with_messages
    from STV2023111337__DWH.h_groups hg 
    inner join STV2023111337__DWH.l_groups_dialogs gd on hg.hk_group_id = gd.hk_group_id 
    inner join STV2023111337__DWH.l_user_message um on um.hk_message_id = gd.hk_message_id 
    inner join STV2023111337__DWH.h_users hu on hu.hk_user_id = um.hk_user_id 
    group by hg.hk_group_id
),
user_group_log as (
    select
        hg.hk_group_id,
        count(distinct luga.hk_user_id) as cnt_added_users
    from (
    select 
        hg_t.hk_group_id 
    from STV2023111337__DWH.h_groups hg_t
    order by hg_t.registration_dt asc
    limit 10
    ) hg
    inner join STV2023111337__DWH.l_user_group_activity luga on luga.hk_group_id = hg.hk_group_id
    inner join STV2023111337__DWH.s_auth_history ah on ah.hk_l_user_group_activity = luga.hk_l_user_group_activity 
    left join STV2023111337__DWH.l_user_message um on um.hk_user_id = luga.hk_user_id 
    where ah.event = 'add'
        and um.hk_user_id is null
    group by hg.hk_group_id    
)

select 
    ugl.hk_group_id,
    ugl.cnt_added_users,
    ugm.cnt_users_in_group_with_messages,
    ugl.cnt_added_users / ugm.cnt_users_in_group_with_messages as group_conversion
from user_group_log as ugl
left join user_group_messages as ugm on ugl.hk_group_id = ugm.hk_group_id
order by ugm.cnt_users_in_group_with_messages / ugl.cnt_added_users desc;