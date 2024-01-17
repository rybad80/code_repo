{{ config(meta = {
    'critical': false
}) }}
/* stg_nursing_dept_cc_p1_other
attempt to match the non inpatient and non Primary Care depts
This is a beta dataset for Nursing; later to be replaced possibly by other code or a lookup
*/
select
    record_status_active_ind,
    dept_key,
    department_name,
    department_abbr,
    department_id,
    scc_abbreviation,
    specialty_name,
    intended_use_id,
    intended_use_abbr,
    intended_use_name,
    care_area_id,
    care_area_abbr,
    care_area_name,
    scc_ind,
    care_network_ind,
    professional_billing_ind,
    location_id,
    revenue_location_group,
    /* 1) direct department id assignments) */
    case department_id
        when 101026006 then '14610' -- BWV URGENT CARE CENTER  14610 Urgent Care Brandywine Valley
        when 101026004 then '14605' -- BUC URGENT CARE CENTER   Urgent Care Bucks County
        when 101026011 then '14625' -- ABINGTON URG CARE CTR  14625 Urgent Care Abington
        when 101026008 then '14615' -- HAVERFORD URG CARE CTR	14615 Urgent Care Haverford
        when 101026001 then '14620' -- MAYS LANDING URG CARE  14620 Urgent Care Mays Landing
        when 101026002 then '34600' -- KOP URGENT CARE CENTER  34600 Urgent Care King of Prussia
        --  14600 Urgent Care King of Prussia  (pre KOPH timeframe)

        when 83347013 then '10501' -- BUC PED GEN THOR SURG  10501 OR Surgical Services Bucks
        when 101013024 then '10502' -- BWV PED GEN THOR SURG  10502 OR Surgical Services Brandywine Valley  
        when 82353013 then '10504' -- VNJ PED GEN THOR SURG  10504 OR Surgical Services Voorhees

        when 101001132 then '10522' -- BWV DAY SURGERY (BWDS) 10522 PACU Brandywine Valley
        when 83417099 then '10521' -- BUC DAY SURGERY (IDS) 10521 PACU Bucks
        when 82424099 then '10524' -- VNJ DAY SURGERY (VDS) 10524 PACU Voorhees

        when 10292012 then '10300' -- MAIN EMERGENCY DEPT  10300 Emergency Department
        when 101003001 then '30300' -- KOPH EMERGENCY DEP  30300 KOP Emergency Department

        when 900100100 then '10520' -- CHOP MAIN PACU 10520 PACU
        when 101003058 then '30520' -- KOPH PACU 30520 KOP PACU
        when 101001069 then '10500' -- PERIOP COMPLEX 10500 OR Surgical Services
        when 84349013 then '30500' -- KOP PED GEN THOR SURG  30500 KOP OR Surgical Services
        -- 10503 OR Surgical Services KOP -- old CC for KOP OR
        -- 10523 PACU KOP -- old cc for KOP PACU
        when 58 then '10540' -- '6 NORTHWEST' 10540 OR Cardiothoracic Surgery

    /* 2) try by specialty when not exact match to cost center name */
        else case upper(dept.specialty_name)
            when 'ENDOCRINOLOGY' then '50300' -- Endocrinology and Diabetes
            when 'PULMONARY' then '55800' --  Pulmonary and Sleep Medicine
            when 'REHAB MEDICINE' then '57600'  -- Rehabilitation Medicine
            when 'GASTROENTEROLOGY' then '10410'  -- GI Endoscopy Suite
            /* (include 'BRYN MAWR GASTRO' 'BUC GASTROENTEROLOGY' 'PNJ GASTROENTEROLOGY'  ?
            then also 30410 KOP GI Endoscopy Suite ) */
            when 'GENETICS' then '52800'  -- Human Genetics
            when 'ORTHOPEDICS' then '76000'  -- Orthopaedics
            when 'ADOLESCENT' then '56000'  -- Adolescent Medicine
            when 'DEVELOPMENTAL PEDIATRICS' then '57100'  -- Developmental and Behavioral Pediatrics
            when 'INFECTIOUS DISEASE' then '53700'  -- Infectious Diseases

    /* 3) last attempt is to assign by matched spciality name in Epic to cost center name */
            else cc_spec_match.cost_center_id
            end
        end as cost_center_id

/* remaining ones to possibly resolve */
--81351013.000000000000000	PNJ PED GEN THOR SURG
--101013015.000000000000000	ABINGTON PED GEN THOR SURGERY
--80348013.000000000000000	EXT PED GEN THOR SURG
--87346013.000000000000000	ATL PED GEN THOR SURG

--101026017 then 'xxx'  --	AB TELEHLTH URG CARE
--101026015 then 'xxx'  --	BWV TELEHLTH URG CARE
--101026014 then 'xxx'  --	KOP TELEHLTH URG CARE
--101026013 then 'xxx'  --	BUC TELEHLTH URG CARE
--101026007 then 'xxx'  --	BWV UC DIAG RAD
--101026005 then 'xxx'  --	BUC URG CARE DIAG RAD

--101001175 then 'xxx'  --	ABINGTON UC LAB
--101001140 then 'xxx'  --	BWV UC LAB
--101001108 then 'xxx'  --	BUC URGENT CARE LAB
from
        {{ ref('stg_department_all') }} as dept
        left join --{{ ref('dim_cost_center') }}
        {{ ref('nursing_cost_center_attributes') }} as cc_spec_match
            on upper(dept.specialty_name) = upper(cc_spec_match.cost_center_name)

where
        (dept.intended_use_name in (
            'Urgent Care',
            'Transport',
            'Home Care',
            'Outpatient Specialty Care' /* 938 departments in Epic */,
            'Perioperative')
            or dept.department_id in (
                10292012, --	MAIN EMERGENCY DEPT	ED
                101003001, -- KOPH EMERGENCY DEP	KED
                900100100, -- CHOP MAIN PACU
                101003058 -- KOPH PACU
                )
        )
        and dept.professional_billing_ind = 0
