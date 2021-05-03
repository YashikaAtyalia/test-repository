WITH BASE AS 
(
select flag.*,adl.relatedprospectid,adl.assigned_date
,adl.activity_time::date as activity_date,
adl.true_owner as ls_owner
,count(case when activityevent = 22 then 1 else null end) no_of_attempts
,count(case when activityevent = 22 AND udh_talk_time>0 then 1 else null end) no_of_connects
,count(distinct case when activityevent = 22 then relatedprospectid else null end) leads_attempted
,count(distinct case when activityevent = 22 AND udh_talk_time>0 then relatedprospectid else null end) leads_connected
--,count(case when activityevent = 22 and flag.baked_stage = 'Attempted' then 1 else null end ) as no_of_attempts_nc_leads
,sum(udh_talk_time) as call_duration
,sum(ch_ringing_time) as ringining_duration
,count(distinct case when activityevent = 502 then relatedprospectid else null end) as no_of_demo_done
,count(distinct case when activityevent = 270 then relatedprospectid else null end) as no_of_orders
,sum(total_student_demo_duration) as demo_duration
,sum(total_wave_barge_duration) as barge_duration
,sum(manger_barge_wave_duration) as manager_barge_duration
,sum(peer_barge_wave_duration) as peer_barge_duration
,count(distinct case when manger_barge_wave_duration is not null then relatedprospectid else null end) as no_of_demos_barged_manager
,count(distinct case when peer_barge_wave_duration is not null then relatedprospectid else null end) as no_of_demos_barged_peer
from analytics_reporting.FOS_IS_Assigned_Activity_ADL_V3 adl
inner join (select prospectassignhistoryid, case when all_stages ilike '%Order_Placed%' then 'Order_Placed'
when all_stages ilike '%Demo_Done%' then 'Demo_Done' else null end as baked_stage
from (select prospectassignhistoryid, listagg(distinct baked_stage,', ' )within group (order by activity_time asc) as all_stages
        from analytics_reporting.FOS_IS_Assigned_Activity_ADL_V3
        where prospectassignhistoryid is not null
        group by 1)
where all_stages ilike '%Demo_Done%' 
group by 1,2 ) flag
on flag.prospectassignhistoryid = adl.prospectassignhistoryid
where (adl.baked_stage = 'Demo_Done'  or ((activity_time = first_order_date) and (first_demo_done_date is not null)))
and adl.prospectassignhistoryid is not null
group by 1,2,3,4,5,6
--having designation in ('ACADEMIC_COUNSELLOR','SENIOR_ACADEMIC_COUNSELLOR','TELE_CALLER','INTERN')
),

Score as
(
select ad2.ls_userid, ad2.emailaddress,ad2.department,ad2.fos_wave_score from analytics_reporting.agent_details ad2 
inner join 
(select ls_userid, emailaddress,department,
MAX(createdonist_date) AS latest_audit_date
from analytics_reporting.agent_details ad2
where fos_wave_score is not null 
GROUP BY 1,2,3) flag
on ad2.ls_userid=flag.ls_userid and ad2.createdonist_date=flag.latest_audit_date
)

SELECT prospectassignhistoryid||relatedprospectid as unique_id, baked_stage,activity_date,
ls.designation,no_of_attempts,no_of_connects,leads_attempted,leads_connected,call_duration,ringining_duration, fos_wave_score,
no_of_demo_done,no_of_orders,demo_duration,barge_duration,manager_barge_duration,peer_barge_duration,no_of_demos_barged_manager,no_of_demos_barged_peer,
LS.emailaddress, name as Agent_name,tl_name,ch_name,rm_name,zm_name,active_flag FROM BASE
left JOIN analytics_reporting.Agent_Hierarchy_Mapping LS
ON BASE.LS_OWNER = LS.USERID
left JOIN score s
ON BASE.LS_OWNER = s.ls_userid 
--where  LS.designation in ('ACADEMIC_COUNSELLOR','SENIOR_ACADEMIC_COUNSELLOR','TELE_CALLER','INTERN')


