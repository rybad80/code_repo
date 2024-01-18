select distinct
    pat.pat_key,
    isnull(census.visit_key, 0) as visit_key,
    dep.dept_key,
    date(census.census_dt) as event_dt,
    'FACT_CENSUS' as denominator_source
from {{ source('cdw', 'fact_census_occ') }} as census
inner join {{ source('cdw', 'patient') }} as pat on census.pat_key = pat.pat_key
inner join {{ source('cdw', 'department') }} as dep on census.dept_key = dep.dept_key
inner join {{ source('cdw', 'master_bed') }} as bed on census.bed_key = bed.bed_key
inner join {{ source('cdw', 'visit_stay_info') }} as vis_info on vis_info.visit_key = census.visit_key
inner join {{ source('cdw', 'flowsheet_record') }} as fsr on vis_info.vsi_key = fsr.vsi_key
inner join {{ source('cdw', 'flowsheet_measure') }} as fsm on fsr.fs_rec_key = fsm.fs_rec_key
inner join {{ source('cdw', 'flowsheet') }} as fs on fsm.fs_key = fs.fs_key
inner join {{ source('cdw', 'patient_lda') }} as lda on lda.pat_key = pat.pat_key
inner join ( --noqa: L042
    select t1.*
    from {{ source('cdw', 'flowsheet_lda_group') }} as t1
    inner join ( --noqa: L042
        select distinct
            fs_key,
            seq_num,
            max(contact_dt_key) over (partition by fs_key, seq_num) as max_date
        from {{ source('cdw', 'flowsheet_lda_group') }}
    ) as t2
        on t1.fs_key = t2.fs_key
        and t1.seq_num = t2.seq_num
        and t1.contact_dt_key = t2.max_date
    where dict_lda_type_key = 20865 --inclusion criteria of LDA Type: Drain
) as ltype
    on lda.fs_key = ltype.fs_key
where pat.pat_key = census.hr_0 --Only count devices used overnight
--REGION inclusion criteria of LDA Type: Ventilators
and fs.fs_id in (40010975, 40010942, 40010965, 40069501, 40002606)--40010975 CHOP R IP RT HFOV POWER SET, 40010942 CHOP R IP RT INVASIVE MODE, 40010965 CHOP R IP RT NON INVASIVE DEVICE, 40069501 CHOP R IP VDR CONVECTIVE RATE SET (CYCLES/ MIN), 40002606 CHOP R IP HFJV PIP
--ENDREGION
and fsm.meas_val is not null
--REGION inclusion criteria of Census Date: when lda is active in CHOP
and (
    (census.census_dt between lda.place_dt and lda.remove_dt)        --Scenario 1: lda is placed and removed in CHOP
    or (lda.place_dt is null and census.census_dt <= lda.remove_dt)  --Scenario 2: lda is placed outside of CHOP
    or (census_dt >= lda.place_dt and lda.remove_dt is null)         --Scenario 3: lda has been not removed yet
    or (lda.place_dt is null and lda.remove_dt is null)              --Scenario 4: lda is placed and removed outside of CHOP             
)
--ENDREGION
and to_char(fsm.rec_dt, 'MM/DD/YYYY') = to_char(census.census_dt, 'MM/DD/YYYY')
and dep.dept_id in (10, 11, 12, 101001070, 46, 101001071, 101001125, 101001126, 36, 34, 51, 43, 123, 101003002, 101003014) --Department: 10 NICA, 11 NICB, 12 NICC, 101001070 NICD, 46 NICE, 101001071 NICF, 101001125 NICG, 101001126 NICH, 36 6ST, 34 7EP, 51 7NE, 43 7ST, 123 7WPICU, 101003002 KIC, 101003014 K5NICU) --To calculate VAP denomintor, we only include these units. Unit cohort could depend on specific project requirement.
--and event_dt < '2022-07-01'
