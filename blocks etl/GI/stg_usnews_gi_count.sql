select distinct
    'operational' as domain, --noqa: L029
    'finance' as subdomain,
     case 
        when usnews_billing.metric_id in ('d13a', 'd13b', 'd13c', 'd13d', 'd13e', 'd13f', 'd13g', 'd13h',
                                        'd13i',
                                        'd11a2',
                                        'd11b2',
                                        'd11c2',
                                        'd11d2',
                                        'd11e2',
                                        'd11f2',
                                        'd11g2',
                                        'd11h2',
                                        'd11i2'
                                        )
                                        then usnews_billing.pat_key
        when
            usnews_billing.metric_id in (
                'd11a3', 'd11b3', 'd11c3', 'd11d3', 'd11e3', 'd11f3', 'd11g3', 'd11h3', 'd11i3', 'd30'
            )
            then
        {{
        dbt_utils.surrogate_key([
            'usnews_billing.pat_key',
            'usnews_billing.service_date',
            'usnews_billing.cpt_code'
        ])
        }}
        when usnews_billing.metric_id = 'd5' and usnews_billing.visit_key > 0
            then usnews_billing.visit_key
        when usnews_billing.metric_id = 'd5' and usnews_billing.visit_key = 0
            then cast(usnews_billing.visit_key||usnews_billing.tx_id as int)
    end as primary_key,
    usnews_billing.service_date as metric_date,
    case 
        when usnews_billing.metric_id in ('d13a', 'd13b', 'd13c', 'd13d', 'd13e', 'd13f', 'd13g', 'd13h',
                                        'd13i',
                                        'd11a2',
                                        'd11b2',
                                        'd11c2',
                                        'd11d2',
                                        'd11e2',
                                        'd11f2',
                                        'd11g2',
                                        'd11h2',
                                        'd11i2'
                                        )
                                        then usnews_billing.pat_key
        when
            usnews_billing.metric_id in (
                'd11a3', 'd11b3', 'd11c3', 'd11d3', 'd11e3', 'd11f3', 'd11g3', 'd11h3', 'd11i3', 'd30'
            )
            then
        {{
        dbt_utils.surrogate_key([
            'usnews_billing.pat_key',
            'usnews_billing.service_date',
            'usnews_billing.cpt_code'
        ])
        }}
        when usnews_billing.metric_id = 'd5' and usnews_billing.visit_key > 0
            then usnews_billing.visit_key
        when usnews_billing.metric_id = 'd5' and usnews_billing.visit_key = 0
            then cast(usnews_billing.visit_key||usnews_billing.tx_id as int)
    end as num,
    usnews_billing.metric_name,
    usnews_billing.metric_id,
    /*used for validation*/
    usnews_billing.submission_year,
    usnews_billing.patient_name,
    usnews_billing.mrn,
    usnews_billing.dob,
    usnews_billing.service_date as index_date,
    usnews_billing.question_number,
    usnews_billing.division,
    usnews_billing.cpt_code,
    usnews_billing.procedure_name,
    '0' as visit_key
from
    {{ ref('usnews_billing') }} as usnews_billing
    inner join {{ ref('lookup_usnews_metadata') }} as lookup_usnews_metadata
        on usnews_billing.question_number = lookup_usnews_metadata.question_number
where
    (usnews_billing.metric_id in (
        'd13a',
        'd13b',
        'd13c',
        'd13d',
        'd13e',
        'd13f',
        'd13g',
        'd13h',
        'd13i',
        'd11a2',
        'd11b2',
        'd11c2',
        'd11d2',
        'd11e2',
        'd11g2',
        'd11h2',
        'd11i2',
        'd11a3',
        'd11b3',
        'd11c3',
        'd11d3',
        'd11e3',
        'd11g3',
        'd11h3',
        'd11i3',
        'd30'
    ) and proc_exclusion_ind = 0)
    or (usnews_billing.metric_id in ('d11f2', 'd11f3')
        and lower(usnews_billing.department_specialty) in (
            'gastroenterology', 'pediatric general thoracic surgery'
        )
        )
    or (usnews_billing.metric_id = 'd5'
        and lower(usnews_billing.department_specialty) = 'gastroenterology')
