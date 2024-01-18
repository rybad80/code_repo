{% set echo_observations = [
    "chw_asd_pfo_obs",
    "pab_asd_location_site_size_obs",
    "aortic_arch_dx_severity_obs",
    "aov_not_eval_nwv_obs",
    "aov_prosthesis_type_obs",
    "arch_sidedness_obs",
    "ca_aneurysm_obs",
    "chw_lsvc_obs",
    "chw_pvn_normal_obs",
    "d_tga_w_vsd_obs",
    "diffuse_ca_dilation_obs",
    "l_tga_w_vsd_obs",
    "lca_obs",
    "mv_structure_and_severity_0_obs",
    "mv_surgeries_obs",
    "pab_ca_fistulae_obs",
    "pab_sys_bilateral_svc_80901_obs",
    "papvc_obs",
    "pulm_veins_not_eval_nwv_obs",
    "rca_obs",
    "tapvc_obs",
    "tof_a_obs",
    "transverse_arch_hypoplasia_obs",
    "tv_structure_and_severity_0_obs"
]
%}
with {% for echo_observation in echo_observations %}
{{echo_observation}} as (
    select
        studyid,
        cast(group_concat(case when lower(n.name) = '{{echo_observation}}'
            then isnull(cast(sfm.worksheetvalue as varchar(400)), cast(val as varchar(400)))
        end, ';') as varchar(400)) as {{echo_observation}}
    from
        {{source('ccis_ods', 'syngo_echo_observationvalue')}} as v
            inner join {{source('ccis_ods', 'syngo_echo_observationname')}} as n
                on v.observationid = n.id
            left join {{source('ccis_ods', 'syngo_echo_observationfieldmap')}} as sfm
                on sfm.observationname = n.name
                and sfm.databasevalue = v.val
    where
        lower(n.name) = '{{echo_observation}}'
    group by
        studyid
), --noqa: L018
{% endfor %}
measurements as (
    select
        studyid,
        median(case when lower(name) = 'aov_ring_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as aortic_annulus,
        median(case when lower(name) = 'aortic_annulus_vs_bsa_boston_11_9_0_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
        end) as aortic_annulus_zscore,
        median(case when lower(name) = 'ao_root_diam_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as aortic_root_diameter,
        median(case when lower(name) = 'aortic_root_vs_bsa_boston_11_9_0_0_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as aortic_root_diameter_zscore,
        median(case when lower(name) = 'ao_st_jnct_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as echo_st_junction,
        median(case when lower(name) = 'aortic_stjunct_vs_bsa_boston_11_9_1_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as echo_st_junction_zscore,
        median(case when lower(name) = 'ao_asc_diam_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as asc_aorta_diameter,
        median(case when lower(name) = 'ascao_vs_bsa_boston_11_9_0_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as asc_aorta_diameter_zscore,
        median(case when lower(name) = 'ao_arch_diam_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as aortic_arch_diameter,
        median(case when lower(name) = 'ao_arch_diam_distal_manual_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as aortic_arch_distal_diameter,
        median(case when lower(name) = 'ao_arch_dist_z_score_phn_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as aortic_arch_distal_zscore,
        median(case when lower(name) = 'ao_isthmus_diam_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as aortic_isthmus_diamater,
        median(case when lower(name) = 'ao_isthmus_vs_bsa_boston_11_9_calc'
            then round(cast(floatvalue as numeric(5, 2)), 2)
            end) as aortic_isthmus_diameter_zscore
    from
        {{source('ccis_ods', 'syngo_echo_measurementvalue')}} as measurementvalue
        inner join {{source('ccis_ods', 'syngo_echo_measurementtype')}} as measurementtype
            on measurementtype.id = measurementvalue.measurementtypeidx
    where lower(name) in (
        'aov_ring_calc',
        'aortic_annulus_vs_bsa_boston_11_9_0_calc',
        'ao_root_diam_calc',
        'aortic_root_vs_bsa_boston_11_9_0_0_calc',
        'ao_st_jnct_calc',
        'aortic_stjunct_vs_bsa_boston_11_9_1_calc',
        'ao_asc_diam_calc',
        'ascao_vs_bsa_boston_11_9_0_calc',
        'ao_arch_diam_calc',
        'ao_arch_diam_distal_manual_calc',
        'taa_vs_bsa_boston_11_9_0_calc',
        'ao_isthmus_diam_calc',
        'ao_isthmus_vs_bsa_boston_11_9_calc',
        'ao_arch_dist_z_score_phn_calc')
        and instancenumber in (0, 1)
    group by
        studyid
),

cardiac_echo as (
    select
        cardiac_echo.cardiac_study_id,
        cardiac_echo.mrn,
        cardiac_echo.patient_name,
        cardiac_echo.study_date
    from
        {{ref('cardiac_echo')}} as cardiac_echo
)

select
    cardiac_echo.cardiac_study_id,
    cardiac_echo.mrn,
    cardiac_echo.patient_name,
    cardiac_echo.study_date,
    measurements.aortic_annulus,
    measurements.aortic_annulus_zscore,
    measurements.aortic_root_diameter,
    measurements.aortic_root_diameter_zscore,
    measurements.echo_st_junction,
    measurements.echo_st_junction_zscore,
    measurements.asc_aorta_diameter,
    measurements.asc_aorta_diameter_zscore,
    measurements.aortic_arch_diameter,
    measurements.aortic_arch_distal_diameter,
    measurements.aortic_arch_distal_zscore,
    measurements.aortic_isthmus_diamater,
    measurements.aortic_isthmus_diameter_zscore,
{%- for echo_observation in echo_observations %}
    {{echo_observation}}.{{echo_observation}} as {{echo_observation | replace('_obs', '') }}
{%- if not loop.last %}, {% endif -%}
{% endfor %}
from
    cardiac_echo
    left join measurements
        on cardiac_echo.cardiac_study_id = measurements.studyid || 'Syn'
{%- for echo_observation in echo_observations -%}
    left join {{echo_observation}}
    on cardiac_echo.cardiac_study_id = {{echo_observation}}.studyid || 'Syn'
{% endfor %}
where
    measurements.aortic_annulus is not null
    or measurements.aortic_annulus_zscore is not null
    or measurements.aortic_root_diameter is not null
    or measurements.aortic_root_diameter_zscore is not null
    or measurements.echo_st_junction is not null
    or measurements.echo_st_junction_zscore is not null
    or measurements.asc_aorta_diameter is not null
    or measurements.asc_aorta_diameter_zscore is not null
    or measurements.aortic_arch_diameter is not null
    or measurements.aortic_arch_distal_diameter is not null
    or measurements.aortic_arch_distal_zscore is not null
    or measurements.aortic_isthmus_diamater is not null
    or measurements.aortic_isthmus_diameter_zscore is not null
{% for echo_observation in echo_observations -%}
    or {{echo_observation}}.{{echo_observation}} is not null
{% endfor %}
