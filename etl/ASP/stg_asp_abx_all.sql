with abx_all as (
    select
        visit_key,
        pat_key,
        encounter_date,
        hospital_admit_date,
        med_ord_key,
        administration_seq_number,
        medication_start_date,
        medication_end_date,
        order_status,
        order_mode,
        order_class,
        medication_order_id,
        med_key,
        medication_id,
        medication_name,
        generic_medication_name,
        medication_order_name,
        order_route,
        order_route_group,
        medication_order_dept_key,
        ordering_department,
        administration_date,
        admin_route,
        admin_route_group,
        medication_administration_dept_key,
        administration_department,
        therapeutic_class,
        administration_type_id,
        -- workhorse function to clean-up disasterous medication names in CDW
        -- first uses medication generic name, when not available, uses medication order name
        -- regular expression attempts to clean-up any antimicrobial-like name
        -- will need periodic validation and updates as/if new antimicrobials become available
        initcap(
            translate(
                array_combine(
                    regexp_extract_all(
                        lower(
                            case when medication_id = 200200490 --ZZ IMS Template
                                then medication_order_name
                                else coalesce(
                                    generic_medication_name,
                                    medication_order_name
                                ) end
                        ),
                        '\b(' -- word boundary

                        --------------------------------------------------------
                        -- SINGLE-PATTERN RULES
                        --------------------------------------------------------
                        || 'amphotericin b( lip\w+)?|'
                        -- MATCHES:
                        -- amphotericin b, amphotericin b lip*

                        || '\w+penem(\-)?(\w+|\b)|'
                        -- MATCHES:
                        -- *penem, *penem*, *penem-, *penem-*

                        || '(co-)?(\w+azole-)?(trim\w+)(sulfa\w+)?|'
                        -- MATCHES:
                        -- *azole-trim*, *azole-trim*sulfa*, co-*azole-trim*, co-*azole-trim*sulfa*, 
                        -- co-trim*, co-trim*sulfa*, trim*, trim*sulfa*

                        || '(\w+)?(pentamid|qu|rilpivir|cr)(ine|inol)|'
                        -- MATCHES:
                        -- *crine, *crinol, *pentamidine, *pentamidinol, *quine, *quinol
                        -- *rilpivirine, *rilpivirinol, crine, crinol, pentamidine, pentamidinol
                        -- quine, quinol, rilpivirine, rilpivirinol

                        || '\w+(ciclo|cyclo|i|a)?vir(in)?|'
                        -- MATCHES:
                        -- *avir, *avirin, *ciclovir, *ciclovirin, *cyclovir, *cyclovirin
                        -- *ivir, *ivirin, *vir, *virin

                        || 'erythro\w+(\-sulfisoxazole)?|'
                        -- MATCHES:
                        -- erythro*, erythro*-sulfisoxazole

                        || '\w+(thenam|closer|zinam|ionam|cytos|danos|fantr|niaz|mect|tham|diaz|vud|naf|uin|vir|rap)(id|in)e?|' -- noqa: L016
                        -- MATCHES:
                        -- *closerid, *closeride, *closerin, *closerine, *cytosid, *cytoside
                        -- *cytosin, *cytosine, *danosid, *danoside, *danosin, *danosine
                        -- *diazid, *diazide, *diazin, *diazine, *fantrid, *fantride
                        -- *fantrin, *fantrine, *ionamid, *ionamide, *ionamin, *ionamine
                        -- *mectid, *mectide, *mectin, *mectine, *nafid, *nafide
                        -- *nafin, *nafine, *niazid, *niazide, *niazin, *niazine
                        -- *rapid, *rapide, *rapin, *rapine, *thamid, *thamide
                        -- *thamin, *thamine, *thenamid, *thenamide, *thenamin, *thenamine
                        -- *uinid, *uinide, *uinin, *uinine, *virid, *viride
                        -- *virin, *virine, *vudid, *vudide, *vudin, *vudine
                        -- *zinamid, *zinamide, *zinamin, *zinamine

                        --------------------------------------------------------
                        -- WHOLE WORD RULES
                        --------------------------------------------------------
                        || '(quinupristin-dalfopristin|praziquantel|artemether|ethambutol|foscarnet|pyrantel|dapsone)|' -- noqa: L016
                        -- MATCHES:
                        -- artemether, dapsone, ethambutol, foscarnet, praziquantel, pyrantel
                        -- quinupristin-dalfopristin

                        --------------------------------------------------------
                        -- WHOLE WORD/WHOLE PREFIX RULES
                        --------------------------------------------------------
                        || '(emtricit|elvit|cobic|tenof|efav)\w*|'
                        -- MATCHES:
                        -- cobic, cobic*, efav, efav*, elvit, elvit*
                        -- emtricit, emtricit*, tenof, tenof*

                        --------------------------------------------------------
                        -- WHOLE SUFFIX RULES
                        --------------------------------------------------------
                        || '\w+(quone-proguanil|bendazole|nidazole|conazole|cycline(?!x)|fungin|chloram|cillin g|cillin v|cillin|quone|zolid|cin|cyn)|' -- noqa: L016
                        -- MATCHES:
                        -- *bendazole, *chloram, *cillin, *cillin g, *cillin v, *cin
                        -- *conazole, *cycline[CANNOT BE FOLLOWED BY "x"], *cyn, *fungin, *nidazole, *quone
                        -- *quone-proguanil, *zolid

                        --------------------------------------------------------
                        -- WHOLE PREFIX RULES
                        --------------------------------------------------------
                        || '(isavucon|rifabut|sulfisox|polymyxi|avibact|colisti|rifaxi|cefotan|rimant|rifamp|griseo|amant|aztre|tazo|clav|sulb|ceph|nit|cef)\w+' -- noqa: L016
                        -- MATCHES:
                        -- amant*, avibact*, aztre*, cef*, cefotan*, ceph*
                        -- clav*, colisti*, griseo*, isavucon*, nit*, polymyxi*
                        -- rifabut*, rifamp*, rifaxi*, rimant*, sulb*, sulfisox*
                        -- tazo*

                        || ')\b', -- word boundary
                        'i' -- case insensitive matching
                    ),
                    '-'
                ),
                '-',
                ' '
            )
        ) as abx_name
    from {{ ref('medication_order_administration') }}
)
select
    abx_all.visit_key,
    abx_all.pat_key,
    abx_all.encounter_date,
    abx_all.hospital_admit_date,
    abx_all.med_ord_key,
    abx_all.administration_seq_number,
    abx_all.medication_start_date,
    abx_all.medication_end_date,
    abx_all.order_status,
    abx_all.order_mode,
    abx_all.order_class,
    abx_all.medication_order_id,
    abx_all.med_key,
    abx_all.medication_id,
    abx_all.medication_name,
    abx_all.generic_medication_name,
    abx_all.medication_order_name,
    abx_all.order_route,
    abx_all.order_route_group,
    abx_all.medication_order_dept_key,
    abx_all.ordering_department,
    abx_all.administration_date,
    abx_all.admin_route,
    abx_all.admin_route_group,
    abx_all.medication_administration_dept_key,
    abx_all.administration_department,
    abx_all.therapeutic_class,
    abx_all.administration_type_id,
    abx_all.abx_name,
    lookup_asp_inpatient_drug_list.drug_category,
    lookup_asp_inpatient_drug_list.drug_class,
    lookup_asp_inpatient_drug_list.drug_subclass,
    lookup_asp_inpatient_drug_list.cdc_drug_ind,
    lookup_asp_inpatient_drug_list.last_line_ind,
    lookup_asp_inpatient_drug_list.targeted_ind,
    lookup_asp_inpatient_drug_list.rule_out_48_hour_ind
from
    abx_all
    left join {{ref('lookup_asp_inpatient_drug_list')}} as lookup_asp_inpatient_drug_list
        on abx_all.abx_name = lookup_asp_inpatient_drug_list.abx_name
