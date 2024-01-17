select
    pfex_all.survey_key as primary_key,
    pfex_all.visit_date,
    case
        when lower(pfex_all.specialty_name) in ('general pediatrics', 'rheumatology', 'endocrinology',
            'gastroenterology', 'allergy', 'pulmonary', 'dermatology', 'cardiology', 'neurology', 'genetics',
            'oncology', 'infectious disease', 'rehab medicine', 'adolescent', 'nephrology',
            'developmental pediatrics', 'gi/nutrition', 'neonatology', 'metabolism', 'hematology',
            'immunology', 'leukodystropy')
            then 1 else 0 end as specialties_incld_ind,
    -- metric name case statement for drill_down
    case
        when lower(pfex_all.survey_line_name) = 'inpatient pediatric'
            and pfex_all.question_id = 'O2'
            then 'IP Staff Worked Well Together Top Box Score'
        when lower(pfex_all.survey_line_name) = 'inpatient pediatric'
            and pfex_all.question_id = 'O4'
            then 'IP Likelihood to Recommend Top Box Score'
        when lower(pfex_all.survey_line_name) = 'specialty care'
            and specialties_incld_ind = 1 --noqa
            and pfex_all.question_id = 'V7'
            then 'Outpatient Informed About Delays Top Box Score'
        when lower(pfex_all.survey_line_name) = 'specialty care'
            and specialties_incld_ind = 1 --noqa
            and pfex_all.question_id = 'O4'
            then 'Outpatient Likelihood to Recommend Top Box Score'
        when lower(pfex_all.survey_line_name) = 'specialty care'
            and specialties_incld_ind = 1 --noqa
            and pfex_all.question_id = 'A1'
            then 'Outpatient Ease of Scheduling Appointments Top Box Score'
        when lower(pfex_all.survey_line_name) = 'specialty care'
            and specialties_incld_ind = 1 --noqa
            and pfex_all.question_id = 'I2'
            then 'Outpatient Sensitivity to Patient Needs'
        when lower(pfex_all.survey_line_name) = 'pediatric ed'
            and pfex_all.question_id = 'F2'
            then 'ED Staff Responsiveness to Your Fears/Concerns Top Box'
        when lower(pfex_all.survey_line_name) = 'pediatric ed'
            and pfex_all.question_id = 'F4'
            then 'ED Likelihood to Recommend Top Box Score'
        when lower(pfex_all.survey_line_name) = 'pediatric ed'
            and pfex_all.question_id = 'A4'
            then 'Waiting Time Before your Child was Brought to the Treatment Area'
        when lower(pfex_all.survey_line_name) = 'specialty care'
            and specialties_incld_ind = 1 --noqa
            and lower(pfex_all.section_name) = 'care provider'
            then 'Outpatient Provider Top Box Score'
        when lower(pfex_all.survey_line_name) = 'inpatient pediatric'
            and lower(pfex_all.section_name) = 'doctors'
            then 'IP Provider Top Box Score'
        when lower(pfex_all.survey_line_name) = 'pediatric ed'
            and lower(pfex_all.section_name) = 'doctors'
            then 'ED Provider Top Box Score' else 'CHECK'
        end as press_ganey_metric_name,
    --numerator
    pfex_all.tbs_ind as numerator,
    --denominator
    1 as denominator
from
    {{ref('pfex_all')}} as pfex_all
where
    pfex_all.comment_ind = 0
    and pfex_all.cms_ind = 0
    and pfex_all.cahps_ind = 0
    and pfex_all.visit_date >= '01/01/2019'
