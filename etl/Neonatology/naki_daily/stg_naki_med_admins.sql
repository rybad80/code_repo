/* original logic found here:
https://github.research.chop.edu/CQI/Safety-SPS-NAKI/blob/patch/bot-replace-text/Code/SQL/NAKI_Cohort_2018.sql
*/

{% set naki_meds = [
    'acyclovir',
    'ambisome',
    'amikacin',
    'amphotericin b',
    'aspirin',
    'captopril',
    'carboplatin',
    'cefotaxime',
    'ceftazidime',
    'cefuroxime',
    'celecoxib',
    'cidofovir',
    'cisplatin',
    'clavulanic',
    'colistimethate',
    'cyclosporine',
    'dapsone',
    'deferasirox',
    'diatrizoate meglumine',
    'diatrizoate sodium',
    'enalapril',
    'enalaprilat',
    'foscarnet',
    'gadopentetate',
    'gadoextate',
    'ganciclovir',
    'gentamicin',
    'ibuprofen',
    'ifosfamide',
    'indomethacin',
    'iodixanol',
    'iohexol',
    'iopamidol',
    'isovue',
    'iopromide',
    'ioversol',
    'ioxaglate meglumine & sodium',
    'ioxilan',
    'ketorolac',
    'lisinopril',
    'lithium',
    'losartan',
    'mesalamine',
    'methotrexate',
    'mitomycin',
    'nafcillin',
    'naproxen',
    'omnipaque',
    'pamidronate disodium',
    'pentamidine',
    'piperacillin',
    'piperacillin-tazobactam',
    'polymixin',
    'sirolimus',
    'sulfasalazine',
    'tacrolimus',
    'tenofovir',
    'ticarcillin',
    'tobramycin',
    'topiramate',
    'valacyclovir',
    'valganciclovir',
    'valsartan',
    'vancomycin',
    'visipaque',
    'zoledronic acid',
    'zonisamide',
] %}

select
    medication_order_administration.visit_key,
    medication_order_administration.administration_date,
    medication_order_administration.medication_name,
    medication_order_administration.generic_medication_name,
    case
        {% for med_name in naki_meds %}
        when lower(medication_name) like '%{{med_name}}%'
            or lower(generic_medication_name) like '%{{med_name}}%'
            then '{{med_name}}'
        {% endfor %}
    end as ntmx_grouper
from
    {{ ref('medication_order_administration') }} as medication_order_administration
    inner join {{ ref('neo_nicu_episode_phl') }} as neo_nicu_episode_phl
        on neo_nicu_episode_phl.visit_key = medication_order_administration.visit_key
where
    ntmx_grouper is not null
    and year(medication_order_administration.administration_date) >= 2018
    and lower(medication_order_administration.order_route_group) in (
        'ecmo circuit',
        'enteral',
        'intraileal',
        'intravascular',
        'intravenous'
    )
    and medication_order_administration.administration_type_id in (
        '1', --given
        '6', --new bag
        '7', --restarted
        '9', --rate change
        '12', --bolus
        '13', --push
        '102', --pt/caregiver admin - non high alert
        '103', --pt/caregiver admin - high alert
        '105', --given by other
        '112', --iv started
        '115', --iv restarted
        '116', --divided dose
        '117', --started by other
        '119', --neb restarted
        '122.0020', --performed
        '127' --bolus from bag/bottle/syringe
    )
