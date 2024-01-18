with interaction_types as (
    select
        flowsheet.fs_key,
        flowsheet.fs_id as flowsheet_id,
        case
            when flowsheet.fs_id in (
                10, --sp02
                14, --weight
                1005, --diastolic_blood_pressure_value
                1004, --systolic_blood_pressure_value
                40000303, --temperature_source
                6, --temperature_c
                9, --respiration
                8, --pulse
                1007, --bsa
                1006, --bmi
                11, --height_cm
                51157, --weight_change_kg
                5657, --secondary_temperature_c
                5658, --secondary_temperature_source
                40000244, --blood_pressure_location
                40000235, --blood_pressure_cuff_size
                40000241 --patient_position        
            ) then 'vitals'
            when flowsheet.fs_id in (
                9,   -- Resp
                10,    --  SpO2
                7631,      -- Barometric Pressure
                7671,      -- Airway Pressure
                7679,      -- PEEP
                7681,      -- PIP
                7737,      -- Tidal Volume
                7745,      -- Minute Volume
                40000234,      -- O2 Flow Rate (Lpm)
                40000242,      -- Resp/O2 Device
                40002468,      -- FiO2 (%)
                40002559,      -- Mean Airway Pressure Actual (cmH2O)
                40002718,      -- Mode, Non-Invasive
                40002725,      -- ETCO2 (mm Hg)
                40003569,      -- Continuous Pulse Ox Site Rotated
                40008116,      -- Artificial Airway
                40010942,      -- Mode
                40010984,      -- HFJV Mean Airway Pressure
                40011001,      -- Mean Airway Pressure Actual (cmH2O)
                40061268,      -- Respiratory (WDL)
                40068114,      -- Airway Secretion Amount
                40068115,      -- Airway Secretion Color and Consistency
                40068126,      -- Secretions/Suction
                40069509      -- VDR MAP (cm H20)        
            ) then 'ventilation'
            when flowsheet.fs_id in (
                40066030,       --assessment
                40071198,    --CHOP R IP GI DIET
                40000080122, --CHOP R IP HUMPTY DUMPTY FALL RISK LEVEL
                40068099,   --CHOP R IP BREATH SOUNDS BILATERAL
                40068100,   --CHOP R IP ABNORMAL BREATH SOUNDS
                40061218,   --CHOP R IP MUSCULOSKELETAL (WDL)
                40061159,   --CHOP R IP PEDS H-ENT (WDL)
                40071489,   --CHOP R IP MEDICAL DEVICE ORTHOTIC SKIN CHECK
                40071284,   --CHOP R IP SKIN WDL
                40061070,   --CHOP R IP PEDS GASTROINTESTINAL (WDL)
                40071468,   --CHOP R IP GENITOURINARY WDL
                40071471,   --CHOP R IP URINE COLOR
                40071356,   --CHOP R IP NEUROVASCULAR WDL
                40069001,   --CHOP R IP PSYCHOSOCIAL WDL
                40069003,   --CHOP R IP FAMILY INVOLVEMENT
                40072358,   --CHOP R IP CARDIOVASCULAR (WDL) TRIGGER
                40060517,   --CHOP R IP PSYCHOSOCIAL ADDITIONAL OBSERVATIONS
                40061091,   --CHOP R IP INFANT-PEDS NEURO ASSESSMENT TRIGGER
                40069002,   --CHOP R IP PATIENT BEHAVIORS/MOOD
                40069004,   --CHOP R IP FAMILY VISIT/LENGTH OF TIME/FAMILY VISITATION
                40071159,   --CHOP R IP PEDS EYES (WDL)
                40061090   --CHOP R IP PEDS NEURO (WDL))                
            ) then 'nursing assessment'
            when flowsheet.fs_id in (
                40010001   -- CHOP G IP LDA PERIPHERAL IV [40010001]
            ) then 'iv documentation'
            when flowsheet.fs_id in(
                110, -- Observations
                16707, -- SpO2 Pulse
                30000093, -- GCS Conversion
                30000094, -- Respiratory Rate
                30000095, -- Systolic BP
                30000096, -- Trauma Score Total
                30000111, -- RUE Strength
                30000112, -- LUE Strength
                30000114, -- RLE Strength
                30000116, -- LLE Strength
                30000238, -- Start/Reset Vitals Timer?
                30000658, -- O2 Device
                40002614, -- Cosign
                40003054, -- Patient Location
                40061101, -- Pupil Assessment
                40061102, -- R Pupil Size (mm)
                40061103, -- R Pupil Reaction
                40061104, -- L Pupil Size (mm)
                40061105, -- L Pupil Reaction
                40061111, -- RUE Sensation
                40061112, -- LUE Sensation
                40061113, -- RLE Sensation
                40061114, -- LLE Sensation
                40071446, -- GCS Eye Opening (Age <2 years)
                40071447, -- GCS Best Verbal Response (Age <2 years)
                40071448, -- GCS Best Motor Response (Age <2 years)
                40071452, -- GCS Eye Opening (Age >=2 years)
                40071453, -- GCS Best Verbal Response (Age >=2 years)
                40071454, -- GCS Best Motor Response (Age >=2 years)
                40077000, -- Glasgow Coma Scale Score (Age <2 Years)
                40077001, -- Glasgow Coma Scale Score (Age >=2 Years)
                40077018, -- GCS Eyes Not Tested Reason (Age <2 years)
                40077019, -- GCS Eyes Not Tested Reason (Age >=2 years)
                40077022, -- GCS Verbal Not Tested Reason (Age <2 years)
                40077023, -- GCS Verbal Not Tested Reason (Age >=2 years)
                40077026, -- GCS Motor Not Tested Reason (Age <2 years)
                40077027, -- GCS Motor Not Tested Reason (Age >=2 years)
                300700796, -- Motor Strength and Sensation (WDL= Strength Strong/Intact Sensation in extremities)
                300700797, -- Motor Strength and Sensation Extremities
                400002615, -- PRN Med Reassessment
                30000142 -- CHOP R ED ACUITY [30000142]
            ) then 'ed vitals'
            when flowsheet.fs_id in (
                30000122   -- CHOP R ED TRIAGE ROOM [30000122]
            ) then 'ED Triage Start'
            when flowsheet.fs_id in (
                19188   -- CHOP R TRAVEL LAST MONTH [19188]
            ) then 'Isolation Status Added'
            when flowsheet.fs_id in (
                30000238   -- CHOP R ED VITALS REASSESSMENT [30000238] 
            ) then 'ED Vitals reassessment'
            when flowsheet.fs_id in (
                30010700   -- CHOP R ED DIRECT CARE PROVIDED [30010700] 
            ) then 'Direct Care Provided'
            when flowsheet.fs_id in (
                3008908   -- CHOP R ED BEHAVIOR RISK ASSESSMENT CURRENT VISIT [3008908]
            ) then 'Behavior Risk Assessment'
        end as flowsheet_interaction_type
    from {{source('cdw', 'flowsheet')}} as flowsheet
)

select
    *
from
    interaction_types
where flowsheet_interaction_type is not null
