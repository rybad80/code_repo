{{ config(meta = {
    'critical': false
}) }}
/* Cart Infusion Date */
select distinct
    id_mrn as patient_mrn,
    upper(id_lname || ', ' || id_fname) as patient_name,
    date(id_dob) as patient_dob,
    cart_infusion_1 as index_date,
    case when (extract(epoch from index_date)
        - extract(epoch from patient_dob)) / (60.0 * 60.0 * 24.00 * 365.25) < 21 then 1 else 0 end
    as lt_21_ind
from {{source('ods','cart_infusions')}}
where
    id_mrn is not null
    and cart_infusion_1 >= '2016-01-01'
union distinct
/* Reinfusion 1 Date */
select distinct
    id_mrn as patient_mrn,
    upper(id_lname || ', ' || id_fname) as patient_name,
    date(id_dob) as patient_dob,
    reinfusion_date_1 as index_date,
    case when (extract(epoch from index_date)
        - extract(epoch from patient_dob)) / (60.0 * 60.0 * 24.00 * 365.25) < 21 then 1 else 0 end
    as lt_21_ind
from {{source('ods','cart_infusions')}}
where
    id_mrn is not null
    and reinfusion_date_1 >= '2016-01-01'
union distinct
/* Reinfusion 2 Date */
select distinct
    id_mrn as patient_mrn,
    upper(id_lname || ', ' || id_fname) as patient_name,
    date(id_dob) as patient_dob,
    reinfusion_date_2 as index_date,
    case when (extract(epoch from index_date)
        - extract(epoch from patient_dob)) / (60.0 * 60.0 * 24.00 * 365.25) < 21 then 1 else 0 end
    as lt_21_ind
from {{source('ods','cart_infusions')}}
where
    id_mrn is not null
    and reinfusion_date_2 >= '2016-01-01'
union distinct
/* Reinfusion 3 Date */
select distinct
    id_mrn as patient_mrn,
    id_lname || ', ' || id_fname as patient_name,
    id_dob as patient_dob,
    reinfusion_date_3 as index_date,
    case when (extract(epoch from index_date)
        - extract(epoch from patient_dob)) / (60.0 * 60.0 * 24.00 * 365.25) < 21 then 1 else 0 end
    as lt_21_ind
from {{source('ods','cart_infusions')}}
where
    id_mrn is not null
    and reinfusion_date_3 >= '2016-01-01'
union distinct
select distinct
    patient_mrn,
    patient_name,
    date(patient_dob) as patient_dob, -- one date has a timestamp
    transplant_date as index_date,
    lt_21_ind
from {{ref('cancer_center_bmt_transplants')}}
where lower(product_type_abbr) = 't cells'
