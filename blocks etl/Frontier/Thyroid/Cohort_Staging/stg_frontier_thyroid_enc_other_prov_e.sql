--4. patients that see these elect clinic providers with specific visit types during the past 3 years
select
    stg_encounter.pat_key,
    stg_encounter.mrn,
    stg_encounter.visit_key,
    stg_encounter.encounter_date,
    case when stg_encounter.visit_type_id in ('2558', --"endo late effects follow up" (elect)
                                            '2554', --"endocrine late effects new"    (elect)
                                            '3419', -- "new survivorship multidisc"   (elect)
                                            '3420', --"fol survivorship multidisc"    (elect)
                                            '2124' -- add "video visit follow up" for courtney
                                            ) then 1 else 0 end as elect_thyroid_ind
from {{ ref('stg_frontier_thyroid_cohort_base_tmp') }} as cohort_base_tmp
inner join {{ ref('stg_encounter')}} as stg_encounter
    on cohort_base_tmp.pat_key = stg_encounter.pat_key
    and year(add_months(stg_encounter.encounter_date, 6)) >= 2020
inner join {{source('cdw','provider')}} as provider
    on provider.prov_key = stg_encounter.prov_key
left join {{ ref('stg_frontier_thyroid_dx_hx') }} as dx_hx
    on cohort_base_tmp.pat_key = dx_hx.pat_key
    and stg_encounter.encounter_date >= dx_hx.thyroid_center_dx_date
where
--type 1) non-elect visits are included
    (provider.prov_id in ('16489', --kivel, courtney g
                                    '5323' --mostoufi moab, sogol
                                    )
        and stg_encounter.visit_type_id in ('2297', --"follow up graves"            (standard)
                                            '2294', --"new graves"                  (standard)
                                            '2289', --"new thyroid nodules"         (standard)
                                            '2290', --"fol up thyroid nodules"      (standard)
                                            '3381', --"fol up thyroid surveillance" (surveillance)
                                            '3380' --"new thyroid surveillance"     (surveillance)
                                            )
    )
--type 2) elect visits of patient who meet dx criteria and happened after the dx
    or (((provider.prov_id = '16489' --kivel, courtney g
        and stg_encounter.visit_type_id in ('2558', --"endo late effects follow up"(elect)
                                            '2554', --"endocrine late effects new" (elect)
                                            '3419', --"new survivorship multidisc" (elect)
                                            '3420', --"fol survivorship multidisc" (elect)
                                            '2124' --add "video visit follow up" for courtney
                                            )
        ) or (provider.prov_id = '5323' --mostoufi moab, sogol
            and stg_encounter.visit_type_id in ('2558', --"endo late effects follow up" (elect)
                                                '2554', --"endocrine late effects new"  (elect)
                                                '3419', --"new survivorship multidisc"  (elect)
                                                '3420' --"fol survivorship multidisc"   (elect)
                                                )
            )
        )
        and dx_hx.pat_key is not null
    )
