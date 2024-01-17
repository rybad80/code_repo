select
        flowsheet_all.pat_key,
        {{
            dbt_utils.surrogate_key([
                'flowsheet_all.pat_key',
                'flowsheet_all.recorded_date'
                ])
        }} as huddle_key,
        flowsheet_all.recorded_date,
        case when flowsheet_all.flowsheet_id = 17759
                then 'staff_present'
            when flowsheet_all.flowsheet_id = 17760
                then 'nurse_supported_ind'
            when flowsheet_all.flowsheet_id = 17761
                then 'bedside_medications'
            when flowsheet_all.flowsheet_id = 17762
                then 'lines_access'
            when flowsheet_all.flowsheet_id = 17763
                then 'intub_extub_plan'
            when flowsheet_all.flowsheet_id = 17764
                then 'roles_identified'
            when flowsheet_all.flowsheet_id = 17765
                then 'testing_needed'
            when flowsheet_all.flowsheet_id = 17766
                then 'testing_other'
            when flowsheet_all.flowsheet_id = 17767
                then 'equipment_needed'
            when flowsheet_all.flowsheet_id = 17768
                then 'blood_product_availability'
            when flowsheet_all.flowsheet_id = 17769
                then 'family_communication_plan'
            when flowsheet_all.flowsheet_id = 17770
                then 'reassess_plan'
            when flowsheet_all.flowsheet_id = 17771
                then 'reassess_other'
            when flowsheet_all.flowsheet_id = 17812
                then 'huddle_needed'
            when flowsheet_all.flowsheet_id = 17813
                then 'no_huddle_reason'
            when flowsheet_all.flowsheet_id = 17816
                then 'location_of_meds_given_during_resuscitation'
            else flowsheet_all.flowsheet_name end as fs_short_nm,
        flowsheet_all.meas_val
    from
        {{ref('flowsheet_all')}} as flowsheet_all
    inner join {{source('cdw', 'patient')}} as patient
        on patient.pat_key = flowsheet_all.pat_key
    where
        flowsheet_all.flowsheet_id in (
            17759, --'People present at bedside'
            17760, --'Nurse is well supported with additional resources for a deteriorating patient'
            17761, --'Medications needed at Bedside'
            17762, --'Lines/Access'
            17763, --'Intubation/Extubation Plan'
            17764, --'Key roles identified if arrest occurs:'
            17765, --'Identify if additional testing is needed'
            17766, --'Other:'
            17767, --'Assess need for equipment in the room '
            17768, --'Assess availability of blood or blood products'
            17769, --'Communication plan with family'
            17770, -- noqa: L016, 'Minimum 2x/day (initial re-evaluation <4 hours after initial identification)'
            17771, --'Other:'
            17812, --'Is a huddle needed?'
            17813, --'Reason?' (enter reason if no huddle is needed)
            17816 --'Location of medications to be given during resuscitation'
        )
