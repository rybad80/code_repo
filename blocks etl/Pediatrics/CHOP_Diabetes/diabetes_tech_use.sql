with tech_baseline as (--OLD workflow FLO snapshot: last updated value as OF 12/31/22
    select
	flowsheet_all.pat_key,
	max(flowsheet_all.encounter_date) as last_flo_encounter_date,
	last_value(case
        when flowsheet_all.flowsheet_id = '10060219'
		then cast(flowsheet_all.meas_val as varchar(64)) end) over (
            partition by
                flowsheet_all.pat_key
            order by
                flowsheet_all.recorded_date
    ) as stg_cgm_type,
	last_value(case
        when flowsheet_all.flowsheet_id = '10060302'
		then cast('1840-12-31' as date) + cast(flowsheet_all.meas_val as int) end) over (
            partition by
                flowsheet_all.pat_key
            order by
                flowsheet_all.recorded_date
    ) as cgm_start_date,
	last_value(case
        when flowsheet_all.flowsheet_id = '10060088'
		then cast(flowsheet_all.meas_val as varchar(64)) end) over (
            partition by
                flowsheet_all.pat_key
            order by
                flowsheet_all.recorded_date
    ) as stg_pump_type,
	min(case
        when flowsheet_all.flowsheet_id = '10060373'
		then date(flowsheet_all.recorded_date)
    end) as pump_start_date,
	last_value(case
        when flowsheet_all.flowsheet_id = '9454'
		then cast(flowsheet_all.meas_val as varchar(64)) end) over (
            partition by
                flowsheet_all.pat_key
            order by
                flowsheet_all.recorded_date
    ) as stg_pump_category
    from
        {{ ref('flowsheet_all') }} as flowsheet_all
    where
        flowsheet_all.flowsheet_id in (
            '10060219', --CGM Brand: CHOP R AMB endO ICR CGM TYPE 
			'10060302', --CGM Start Date: CHOP R AMB endO ICR CGM START DATE 
			'10060088', --Pump Brand: CHOP R AMB endO DM CURRENT INSULIN PUMP 
			'10060373', --Insulin Pump Start Date (free typing row)
			'9454' --Pump Category: CHOP R AMB endO INSULIN PUMP SENSOR AUGMENTED FEATURE CASCADE 2 (SINGLE)
		)
        and date(flowsheet_all.recorded_date) <= '2022-12-31'
    group by
        flowsheet_all.pat_key,
        flowsheet_all.flowsheet_id,
        flowsheet_all.meas_val,
        flowsheet_all.recorded_date
),

tech_fact as (--NEW workflow SDE
    select
        smart_data_element_all.pat_key,
        smart_data_element_all.mrn,
        --CGM SDE:
        case
            when smart_data_element_all.concept_id = 'CHOP#7250'
            then cast(smart_data_element_all.element_value as varchar(64))
        end as has_cgm,
        case
            when smart_data_element_all.concept_id = 'CHOP#7253'
                --Epic builder fixed the SDE mapping issue IN April 20293
                and date(smart_data_element_all.entered_date) > '2023-04-30'
            then cast(smart_data_element_all.element_value as varchar(64))
            when smart_data_element_all.concept_id = 'CHOP#7253'
                and lower(smart_data_element_all.element_value) in (
                    'dexcom',
                    'freestyle',
                    'medtronic',
                    'other'
                )
                and date(smart_data_element_all.entered_date) <= '2023-04-30'
            then cast(smart_data_element_all.element_value as varchar(64))
        end as stg_cgm_type,
        case
            when smart_data_element_all.concept_id = 'CHOP#7486'
                and cast(smart_data_element_all.element_value as double) is not null
            then smart_data_element_all.element_value
        end as use_cgm_frequency, --threshold: MORE than 80
        case
            when smart_data_element_all.concept_id = 'CHOP#7941'
            then cast(smart_data_element_all.element_value as varchar(64))
        end as cgm_interpreted_today,
        case
            when smart_data_element_all.concept_id = 'CHOP#7251'
            then cast('1840-12-31' as date) + cast(smart_data_element_all.element_value as int)
        end as cgm_start_date,
        case
            when smart_data_element_all.concept_id = 'CHOP#7277'
            then cast(smart_data_element_all.element_value as varchar(64))
        end as stg_cgm_date_range,
        case
            when smart_data_element_all.concept_id = 'CHOP#7398'
            then cast(smart_data_element_all.element_value as double)
        end as stg_cgm_above_range,
        case
            when smart_data_element_all.concept_id = 'CHOP#7401'
            then cast(smart_data_element_all.element_value as double)
        end as stg_cgm_within_range,
        case
            when smart_data_element_all.concept_id = 'CHOP#7399'
            then cast(smart_data_element_all.element_value as double)
        end as stg_cgm_below_range,
        --Pump SDE:
        case
            when smart_data_element_all.concept_id = 'CHOP#7244'
            then cast(smart_data_element_all.element_value as varchar(64))
        end as stg_pump_type, --pump brand (multi-response row)
        case
            when smart_data_element_all.concept_id = 'CHOP#7265'
            then cast('1840-12-31' as date) + cast(smart_data_element_all.element_value as int)
        end as pump_start_date,
        case
            when smart_data_element_all.concept_id = 'CHOP#7247'
            then cast(smart_data_element_all.element_value as varchar(64))
        end as stg_pump_category, --pump category: SENSOR AUGMENTED FEATURES
        coalesce(smart_data_element_all.encounter_date,
            date(smart_data_element_all.entered_date)) as final_encounter_date
    from
        {{ ref('smart_data_element_all') }} as smart_data_element_all
    where
        smart_data_element_all.concept_id in (
        --CGM SDEs:
			--LQF1700	ICR Smartform: CHOP AMB SF endO DIABETES PAT: 
			'CHOP#7250', --PATIENT Has A CGM
            'CHOP#7253', --CGM TYPE
            'CHOP#7941', --CGM INTERPRETED TODAY
            'CHOP#7277', --CGM DATE RANGE
			'CHOP#7486', -- FREQUENCY OF USE NUMBER
			'CHOP#7398', --HYPERGLYCEMIA ANALYSIS (above range)
			'CHOP#7401', --WITHIN RANGE ANALYSIS
			'CHOP#7399', --HYPOGLYCEMIA ANALYSIS (below range)
            'CHOP#7251', --CGM START DATE --Educator Smartform ONLY
		--Pump SDEs:
			--LQF1700	ICR Smartform: CHOP AMB SF endO DIABETES PAT: 
			'CHOP#7244', --CURRENT INSULIN PUMP --Pump Brand
			'CHOP#7247', --SENSOR AUGMENTED FEATURES --Pump Category
			--LQF1713	Educator Smartform: CHOP AMB SF endO DIABETES RD EDUCATORS			
			'CHOP#7265' --INSULIN PUMP START DATE
		)
)

select
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	diabetes_patient_all.mrn,
	coalesce(tech_fact.final_encounter_date,
		--if there's no tech visit since CY2023 then pull last encounter date as of 12/31/22
			max(tech_baseline.last_flo_encounter_date)) as final_encounter_date,
	--CGM fields:
	max(tech_fact.has_cgm) as has_cgm,
	coalesce(max(case
        when tech_fact.final_encounter_date  <= '2023-04-30'
			and tech_fact.stg_cgm_type not in (
                'Dexcom',
                'Freestyle',
                'MedTronic',
                'Other'
			)
		then null
        else tech_fact.stg_cgm_type
    end), max(tech_baseline.stg_cgm_type)) as cgm_type,
	case
        when cgm_type is not null
		then row_number() over (
            partition by
                case when cgm_type is not null
                    then diabetes_patient_all.diabetes_reporting_month end,
                case when cgm_type is not null
                    then diabetes_patient_all.patient_key end
            order by
                tech_fact.final_encounter_date desc
    ) end as cgm_type_rn,
	case
        when cgm_type_rn = 1 then 1
		when cgm_type_rn is not null then 0
    end as current_cgm_type_ind,
	max(tech_fact.use_cgm_frequency) as use_cgm_frequency,
	max(tech_fact.cgm_interpreted_today) as cgm_interpreted_today,
	coalesce(max(tech_fact.cgm_start_date), max(tech_baseline.cgm_start_date)) as cgm_start_date,
	max(tech_fact.stg_cgm_date_range) as cgm_date_range,
	max(tech_fact.stg_cgm_above_range) as cgm_above_range,
	max(tech_fact.stg_cgm_within_range) as cgm_within_range,
	max(tech_fact.stg_cgm_below_range) as cgm_below_range,
	case
        when coalesce(cgm_above_range, cgm_within_range, cgm_below_range) is not null
            then row_number() over (
                partition by
                    case when coalesce(cgm_above_range, cgm_within_range, cgm_below_range) is not null
						then diabetes_patient_all.diabetes_reporting_month end,
					case when coalesce(cgm_above_range, cgm_within_range, cgm_below_range) is not null
						then diabetes_patient_all.patient_key end
				order by
                    tech_fact.final_encounter_date desc
    ) end as cgm_range_analysis_rn,
	case
        when cgm_range_analysis_rn = 1 then 1
		when cgm_range_analysis_rn is not null then 0
    end as current_cgm_range_analysis_ind,
	--pump fields:
	coalesce(max(tech_fact.stg_pump_type), max(tech_baseline.stg_pump_type)) as pump_type,
	case when pump_type is not null
		then row_number() over (
            partition by
                case when pump_type is not null
                    then diabetes_patient_all.diabetes_reporting_month end,
				case when pump_type is not null
                    then diabetes_patient_all.patient_key end
			order by tech_fact.final_encounter_date desc
    ) end as pump_type_rn,
	case
        when pump_type_rn = 1 then 1
		when pump_type_rn is not null then 0
    end as current_pump_type_ind,
	coalesce(max(tech_fact.pump_start_date), max(tech_baseline.pump_start_date)) as pump_start_date,
	coalesce(max(tech_fact.stg_pump_category), max(tech_baseline.stg_pump_category)) as pump_category,
	case
        when pump_category is not null
		then row_number() over (
            partition by
                case when pump_category is not null
                    then diabetes_patient_all.diabetes_reporting_month end,
                case when pump_category is not null
                    then diabetes_patient_all.patient_key end
			order by
                tech_fact.final_encounter_date desc
    ) end as pump_category_rn,
	case
        when pump_category_rn = 1 then 1
		when pump_category_rn is not null then 0
    end as current_pump_category_ind
from
	{{ ref('diabetes_patient_all') }} as diabetes_patient_all
    left join tech_fact
        on diabetes_patient_all.pat_key = tech_fact.pat_key
        --include CGM/Pump info in the past 36 months
        and tech_fact.final_encounter_date >= diabetes_patient_all.diabetes_reporting_month - interval('3 year')
		and tech_fact.final_encounter_date <= diabetes_patient_all.diabetes_reporting_month
    left join tech_baseline
        on diabetes_patient_all.pat_key = tech_baseline.pat_key
where
	coalesce(tech_fact.pat_key, tech_baseline.pat_key) is not null
	and diabetes_patient_all.diabetes_reporting_month >= '2019-07-01'
group by
	diabetes_patient_all.diabetes_reporting_month,
	diabetes_patient_all.patient_key,
	diabetes_patient_all.mrn,
	tech_fact.final_encounter_date
