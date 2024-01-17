with purchase_order as (
    select
        workday_purchase_orders.po_number as document_number,
        workday_purchase_orders.purchase_order_id as document_id,
        workday_purchase_orders.purchase_order_wid as document_wid,
        workday_purchase_orders.po_number as purchase_order_number,
        null as invoice_number,
        cast(workday_purchase_orders.purchase_order_date as date) as document_date,
        'purchase order' as transaction_type,
        case when lower(workday_purchase_orders.po_number) like '%rsub' then 'research'
             when strleft(workday_purchase_orders.po_number, 1) = '2' then 'coupa'
             else 'workday' end as order_code,
        case when lower(workday_supplier_contract.contract_name) like '%punch%' then 'punchout'
             when lower(workday_supplier_contract.contract_name) is not null then 'contract'
             when workday_purchase_orders_lines.purchase_item_id is not null then 'item master'
             else 'off contract' end as order_spend_source,
        workday_purchase_orders.purchase_order_status_id as document_status_id,
        workday_purchase_orders.purchase_order_version_number as order_version_number,
        workday_purchase_orders.issue_option_id as order_issue_option_descriptor,
        case workday_purchase_orders.purchase_order_type_id
            when 'ORDER_TYPE-6-1' then 'Bill Only'
            when 'ORDER_TYPE-3-9' then 'Inventory Replenishment'
            when 'ORDER_TYPE-3-8' then 'ParEx Replenishment'
            when 'ORDER_TYPE-3-10' then 'SLC Mezzanine Replenishment'
            when 'ORDER_TYPE-3-7' then 'PAR Replenishment'
            when 'ORDER_TYPE-3-11' then 'SLC Bulk Replenishment'
            when 'ORDER_TYPE-3-6' then 'Medline Carousel Replenishment'
            when 'ORDER_TYPE-6-3' then 'R-Sub'
            else workday_purchase_orders.purchase_order_type_id end as order_type_descriptor,
        -- all the classification/accounting tags: supplier, company, supplier contract, cost center, location
        stg_purchase_invoice_worker.display_name_formatted as order_buyer_name,
        workday_purchase_orders.company_id,
        workday_purchase_orders.supplier_id,
        workday_purchase_orders.supplier_wid,
        workday_supplier_contract.supplier_contract_number
        || ': ' || workday_supplier_contract.contract_name as supplier_contract_descriptor,
        workday_purchase_orders_lines.supplier_contract_id,
        workday_purchase_orders_lines.cost_center_id,
        workday_purchase_orders_lines.cost_center_site_id,
        workday_purchase_orders_lines.fund_id,
        workday_purchase_orders_lines.grant_id,
        workday_purchase_orders_lines.program_id,
        workday_purchase_orders_lines.project_id,
        workday_purchase_orders_lines.provider_id,
        workday_purchase_orders_lines.spend_category_id,
        workday_purchase_orders_lines.ship_to_address_id,
        workday_purchase_orders_lines.location_id as par_location_id,
        -- contact information and memo
        ship_to_contact_worker.display_name_formatted as ship_to_contact_descriptor,
        bill_to_contact_worker.display_name_formatted as bill_to_contact_descriptor,
        workday_purchase_orders.ship_to_contact_worker_wid,
        workday_purchase_orders.memo as document_memo,
        -- payment terms
        workday_purchase_orders_lines.requisition_type as requisition_type_descriptor,
        workday_purchase_orders_lines.deliver_to_location as deliver_to_location_descriptor,
        workday_purchase_orders_lines.location_for_po_line_distribution
        as location_for_po_line_distribution_descriptor,
        payment_terms.payment_terms_name as payment_terms_descriptor,
        workday_purchase_orders.shipping_terms_id as shipping_terms_descriptor,
        -- some invoice fields
        null as invoice_listed_po_wid,
        null as invoice_listed_po_line_nbr,
        null as invoice_listed_external_po_number,
        null as invoice_listed_invoice_number,
        null as invoice_document_link,
        -- Item level information starts here
        case  when workday_purchase_orders_lines.is_goods_line = 1 then 'goods'
        when workday_purchase_orders_lines.is_service_line = 1 then 'services'
        else 'others' end as goods_vs_services_flag,
        -- Item level description
        workday_purchase_orders_lines.purchase_order_line_id,
        null as invoice_line_id,
        workday_purchase_orders_lines.purchase_item_id,
        workday_purchase_orders_lines.purchase_item_wid,
        workday_purchase_orders_lines.line_item_description as document_line_description,
        workday_purchase_orders_lines.related_purchase_item_for_document_line as document_line_item_description,
        -- line cost, quantity, uom and $ amount
        workday_purchase_orders_lines.quantity,
        workday_purchase_orders_lines.quantity_received,
        workday_purchase_orders_lines.quantity_invoiced,
        workday_purchase_orders_lines.unit_of_measure as order_line_unit_of_measure,
        workday_purchase_orders_lines.line_memo as order_line_memo,
        workday_purchase_orders_lines.purchase_order_line_receipt_status_id as line_receipt_status_descriptor,
        workday_purchase_orders_lines.purchase_order_line_fully_received as purchase_order_line_fully_received_ind,
        -- purchase line dollar amount
        workday_purchase_orders_lines.unit_cost,
        workday_purchase_orders_lines.extended_amount as purchase_line_amount,
        workday_purchase_orders_lines.actual_amount_invoiced as purchase_line_invoice_amount,
        -- invoice line dollar amount
        null as invoice_line_amount,
        null as invoice_currency_adjusted_line_amount,
        -- other invoice dollar fields
        null as invoice_freight_amount,
        null as invoice_gross_invoice_amount,
        null as invoice_document_currency_conversion_rate,
        null as invoice_currency_rate_lookup_override,
        null as invoice_currency_id,
        null as invoice_other_charges
    from
         {{source('workday_ods', 'workday_purchase_orders')}} as workday_purchase_orders
        inner join {{source('workday_ods', 'workday_purchase_orders_lines')}} as workday_purchase_orders_lines
            on workday_purchase_orders_lines.purchase_order_wid = workday_purchase_orders.purchase_order_wid
        left join {{source('workday_ods', 'workday_supplier_contract')}} as workday_supplier_contract
            on workday_supplier_contract.supplier_contract_wid
            = workday_purchase_orders_lines.supplier_contract_wid
        left join {{ref('stg_purchase_invoice_worker')}} as stg_purchase_invoice_worker
            on stg_purchase_invoice_worker.worker_wid = workday_purchase_orders.buyer_worker_wid
        left join {{ref('stg_purchase_invoice_worker')}} as ship_to_contact_worker
            on ship_to_contact_worker.worker_wid = workday_purchase_orders.ship_to_contact_worker_wid
        left join {{ref('stg_purchase_invoice_worker')}} as bill_to_contact_worker
            on bill_to_contact_worker.worker_wid = workday_purchase_orders.bill_to_contact_worker_wid
        left join {{source('workday_ods', 'payment_terms')}} as payment_terms
            on payment_terms.payment_terms_wid = workday_purchase_orders.payment_terms_wid
),
invoice_document as (
    select
        workday_supplier_invoices.invoice_number as document_number,
        workday_supplier_invoices.supplier_invoice_id as document_id,
        workday_supplier_invoices.supplier_invoice_wid as document_wid,
        purchase_order.purchase_order_number,
        workday_supplier_invoices.invoice_number as invoice_number,
        cast(workday_supplier_invoices.invoice_date as date) as document_date,
        'invoice document' as transaction_type,
        purchase_order.order_code,
        purchase_order.order_spend_source,
        -- flags, buyer, version number and order status
        workday_supplier_invoices.invoice_status_id as document_status_id,
        purchase_order.order_version_number,
        purchase_order.order_issue_option_descriptor,
        purchase_order.order_type_descriptor,
        purchase_order.order_buyer_name,
        -- all the classification/accounting tags: supplier, company, supplier contract, cost center, location
        workday_supplier_invoices.company_id,
        workday_supplier_invoices.supplier_id,
        workday_supplier_invoices.supplier_wid, --remove later
        purchase_order.supplier_contract_descriptor,
        purchase_order.supplier_contract_id,
        workday_supplier_invoices_lines.cost_center_id,
        workday_supplier_invoices_lines.cost_center_site_id,
        workday_supplier_invoices_lines.fund_id,
        workday_supplier_invoices_lines.grant_id,
        workday_supplier_invoices_lines.program_id,
        workday_supplier_invoices_lines.project_id,
        workday_supplier_invoices_lines.provider_id,
        workday_supplier_invoices_lines.spend_category_id,
        workday_supplier_invoices_lines.ship_to_address_id as ship_to_address_id,
        workday_supplier_invoices_lines.location_id as par_location_id,
        -- contact information and memo
        purchase_order.ship_to_contact_descriptor,
        purchase_order.bill_to_contact_descriptor,
        purchase_order.ship_to_contact_worker_wid,
        workday_supplier_invoices.memo as document_memo,
        -- payment terms
        purchase_order.requisition_type_descriptor,
        purchase_order.deliver_to_location_descriptor,
        purchase_order.location_for_po_line_distribution_descriptor,
        purchase_order.payment_terms_descriptor,
        purchase_order.shipping_terms_descriptor,
        -- invoice fields
        workday_supplier_invoices_lines.purchase_order_wid as invoice_listed_po_wid,
        workday_supplier_invoices_lines.purchase_order_line_id as invoice_listed_po_line_nbr,
        workday_supplier_invoices.external_po_number as invoice_listed_external_po_number,
        workday_supplier_invoices.supplier_invoice_number as invoice_listed_invoice_number,
        workday_supplier_invoices.document_link as invoice_document_link,
        purchase_order.goods_vs_services_flag,
        -- Item level description
        purchase_order.purchase_order_line_id,
        workday_supplier_invoices_lines.line_order as invoice_line_id,
        purchase_order.purchase_item_id,
        workday_supplier_invoices_lines.purchase_item_wid,
        purchase_order.document_line_description,
        purchase_order.document_line_item_description,
        -- line cost, quantity, uom and $ amount
        workday_supplier_invoices_lines.quantity,
        purchase_order.quantity_received,
        purchase_order.quantity_invoiced,
        null as order_line_unit_of_measure,
        purchase_order.order_line_memo,
        null  as line_receipt_status_descriptor,
        null  as purchase_order_line_fully_received_ind,
        -- purchase line dollar amount
        purchase_order.unit_cost,
        null as purchase_line_amount,
        null as purchase_line_invoice_amount,
        -- invoice dollar amount
        --ignoring the length for invoice_currency_adjusted_line_amount column, will let the team decide later
        workday_supplier_invoices_lines.invoice_line_extended_amount_with_sign as invoice_line_amount,
        case when coalesce(
            workday_supplier_invoices.currency_rate_lookup_override,
            workday_supplier_invoices.currency_exchange_rate) != 0.0
             then coalesce(
                workday_supplier_invoices.currency_rate_lookup_override,
                workday_supplier_invoices.currency_exchange_rate)
                    * workday_supplier_invoices_lines.invoice_line_extended_amount_with_sign
             else workday_supplier_invoices_lines.invoice_line_extended_amount_with_sign
        end as invoice_currency_adjusted_line_amount,
        workday_supplier_invoices.freight_amount as invoice_freight_amount,
        workday_supplier_invoices.invoice_amount_including_intercompany as invoice_gross_invoice_amount,
        workday_supplier_invoices.currency_exchange_rate as invoice_document_currency_conversion_rate,
        workday_supplier_invoices.currency_rate_lookup_override as invoice_currency_rate_lookup_override,
        workday_supplier_invoices.currency_for_supplier_contract as invoice_currency_id,
        workday_supplier_invoices.other_charges as invoice_other_charges
    from
        {{source('workday_ods', 'workday_supplier_invoices')}} as workday_supplier_invoices
        inner join
            {{source('workday_ods', 'workday_supplier_invoices_lines')}} as workday_supplier_invoices_lines
            on workday_supplier_invoices_lines.supplier_invoice_wid
            = workday_supplier_invoices.supplier_invoice_wid
        left join purchase_order
            on purchase_order.document_wid = workday_supplier_invoices_lines.purchase_order_wid
            and purchase_order.purchase_order_line_id = workday_supplier_invoices_lines.purchase_order_line_id
),
combination_cte as (
    select
        *
    from
        purchase_order
    union all
    select
        *
    from
        invoice_document
),
supplier_with_contract_item_fy as (
    select
        master_date.f_yyyy as contract_item_fy_financial_year,
        purchase_order.supplier_id as contract_item_fy_supplier_id
    from
        purchase_order
        inner join {{source('cdw', 'master_date')}} as master_date
            on master_date.full_dt = purchase_order.document_date
    where
        supplier_contract_descriptor is not null
    group by
        master_date.f_yyyy, purchase_order.supplier_id
)

select --unique and type order fields
    combination_cte.document_number,
    combination_cte.transaction_type,
    combination_cte.purchase_order_number,
    combination_cte.invoice_number,
    combination_cte.document_id,
    combination_cte.document_wid,
    combination_cte.document_date,
    master_date.f_yyyy as financial_year, --combination add
    master_date.c_yyyy as calendar_year, --combination add

    combination_cte.order_code,
    combination_cte.order_spend_source,
    combination_cte.document_status_id,

    -- version number and order status
    combination_cte.order_version_number,
    combination_cte.order_issue_option_descriptor,
    combination_cte.order_type_descriptor,

    -- all the accounting fields
    -- supplier, company, supplier contract, cost center, location
    combination_cte.order_buyer_name,
    company.company_name, --combination add
    combination_cte.company_id,
    supplier.supplier_name, --combination add
    combination_cte.supplier_id,
    combination_cte.supplier_wid,
    combination_cte.supplier_contract_descriptor,
    combination_cte.supplier_contract_id,
    cost_center.cost_center_name, --combination add
    combination_cte.cost_center_id,
    cost_center_site.cost_center_site_name, --combination add
    combination_cte.cost_center_site_id,
    fund.fund_name, --combination add
    combination_cte.fund_id,
    grants.grant_name, --combination add
    combination_cte.grant_id,
    program.program_name, --combination add
    combination_cte.program_id,
    project.project_name, --combination adsd
    combination_cte.project_id,
    provider.provider_name, --combination add
    combination_cte.provider_id,
    spend_category.spend_category_name, --combination add
    combination_cte.spend_category_id,
    stg_purchase_invoice_address_location.ship_to_address_location,  --combination add
    stg_purchase_invoice_address_location.ship_to_address_name,  --combination add
    combination_cte.ship_to_address_id,
    location.location_name as par_location_name, --combination add
    combination_cte.par_location_id,
    -- contact information
    combination_cte.ship_to_contact_descriptor,
    combination_cte.bill_to_contact_descriptor,
    combination_cte.ship_to_contact_worker_wid,
    combination_cte.document_memo,
    combination_cte.requisition_type_descriptor,
    combination_cte.deliver_to_location_descriptor,
    combination_cte.location_for_po_line_distribution_descriptor,
    combination_cte.payment_terms_descriptor,
    combination_cte.shipping_terms_descriptor,
    combination_cte.invoice_listed_po_wid,
    combination_cte.invoice_listed_po_line_nbr,
    combination_cte.invoice_listed_external_po_number,
    combination_cte.invoice_listed_invoice_number,
    combination_cte.invoice_document_link,
    combination_cte.goods_vs_services_flag,
    -- Item level information
    combination_cte.purchase_order_line_id,
    combination_cte.invoice_line_id,
    combination_cte.purchase_item_id,
    combination_cte.purchase_item_wid,
    combination_cte.document_line_description,
    combination_cte.document_line_item_description,
    stg_purchase_invoice_item_supplier.supplier_item_identifier,
    purchase_item.manufacturer_id,
    purchase_item.manufacturer_name,
    purchase_item.mfg_part_num,
    --line cost, quantity, uom and $ amount
    combination_cte.quantity,
    combination_cte.quantity_received,
    combination_cte.quantity_invoiced,
    combination_cte.order_line_unit_of_measure,
    combination_cte.order_line_memo,
    combination_cte.line_receipt_status_descriptor,
    combination_cte.purchase_order_line_fully_received_ind,
    -- unit cost
    combination_cte.unit_cost,
    stg_purchase_invoice_current_price.unit_price_as_of_effective_date as current_unit_cost,
    combination_cte.purchase_line_amount,
    combination_cte.purchase_line_invoice_amount,
    sum(combination_cte.purchase_line_amount) over
    (partition by combination_cte.purchase_order_number) as total_purchase_line_amount,
    -- invoice dollar amount
    combination_cte.invoice_line_amount,
    combination_cte.invoice_currency_adjusted_line_amount,
    sum(combination_cte.invoice_currency_adjusted_line_amount) over
    (partition by combination_cte.invoice_number) as total_invoice_currency_adjusted_line_amount,
    count(*) over
    (partition by combination_cte.invoice_number) as total_number_of_invoice_lines,
    -- other invoice fields
    combination_cte.invoice_freight_amount,
    combination_cte.invoice_gross_invoice_amount,
    combination_cte.invoice_document_currency_conversion_rate,
    combination_cte.invoice_currency_rate_lookup_override,
    combination_cte.invoice_currency_id,
    combination_cte.invoice_other_charges,
    case when supplier_with_contract_item_fy.contract_item_fy_supplier_id is not null then 'true'
         else 'false' end as financial_year_supplier_contract_flag
from
    combination_cte
    inner join {{source('cdw', 'master_date')}} as master_date
        on master_date.full_dt = combination_cte.document_date
    left join {{source('workday_ods', 'supplier')}} as supplier
        on supplier.supplier_wid = combination_cte.supplier_wid
    left join {{source('workday_ods', 'company')}} as company
        on company.company_id = combination_cte.company_id
    left join {{source('workday_ods', 'cost_center')}} as cost_center
        on cost_center.cost_center_id = combination_cte.cost_center_id
    left join {{source('workday_ods', 'cost_center_site')}} as cost_center_site
        on cost_center_site.cost_center_site_id = combination_cte.cost_center_site_id
    left join {{source('workday_ods', 'fund')}} as fund
        on fund.fund_id = combination_cte.fund_id
    left join {{source('workday_ods', 'grants')}} as grants -- noqa: L029
        on grants.grant_id = combination_cte.grant_id
    left join {{source('workday_ods', 'project')}} as project
        on project.project_id = combination_cte.project_id
    left join {{source('workday_ods', 'program')}} as program
        on program.program_id = combination_cte.program_id
    left join {{source('workday_ods', 'provider')}} as provider
        on provider.provider_id = combination_cte.provider_id
    left join {{source('workday_ods', 'purchase_item')}} as purchase_item
        on purchase_item.purchase_item_wid = combination_cte.purchase_item_wid
    left join {{ref('stg_purchase_invoice_item_supplier')}} as stg_purchase_invoice_item_supplier
        on stg_purchase_invoice_item_supplier.supplier_id = combination_cte.supplier_id
        and stg_purchase_invoice_item_supplier.purchase_item_id = combination_cte.purchase_item_id
    left join {{source('workday_ods', 'spend_category')}} as spend_category
        on spend_category.spend_category_id = combination_cte.spend_category_id
    left join {{source('workday_ods', 'location')}} as location -- noqa: L029
        on location.location_id = combination_cte.par_location_id
    left join {{ref('stg_purchase_invoice_address_location')}} as stg_purchase_invoice_address_location
        on stg_purchase_invoice_address_location.ship_to_address_id = combination_cte.ship_to_address_id
    left join supplier_with_contract_item_fy
        on supplier_with_contract_item_fy.contract_item_fy_financial_year = master_date.f_yyyy
        and supplier_with_contract_item_fy.contract_item_fy_supplier_id = combination_cte.supplier_id
    left join {{ref('stg_purchase_invoice_current_price')}} as stg_purchase_invoice_current_price
        on stg_purchase_invoice_current_price.purchase_item_id = combination_cte.purchase_item_id
        and stg_purchase_invoice_current_price.unit_of_measure_descriptor
        = combination_cte.order_line_unit_of_measure
        and stg_purchase_invoice_current_price.item_unit_of_measure_latest_price_with_primary_source_row_number = 1
        and stg_purchase_invoice_current_price.identifier_unit_of_measure_latest_price_row_number = 1
