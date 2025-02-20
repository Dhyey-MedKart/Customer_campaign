import datetime

ASSURED_QUERY = '''
    WITH branded_chronic AS (
        SELECT 
            p.product_name AS bc_product_name,
            p.product_code AS bc_product_code, 
            p.mrp AS bc_mrp, 
            p.is_assured AS bc_is_assured, 
            p.is_rx_required AS bc_is_rx_required, 
            p.is_chronic AS bc_is_chronic, 
            p.is_refrigerated AS bc_is_refrigerated, 
            p.mis_reporting_category AS bc_catg, 
            pu.content AS bc_dosage_form, 
            pu.package_type AS bc_package_type, 
            pu.uom AS bc_uom, 
            pu.package_size AS bc_package_size,
            p.combinations_string AS bc_combinations_string,
            pp.sales_price AS bc_sales_price
        FROM 
            products p
        LEFT JOIN 
            product_units pu ON pu.product_id = p.id
        LEFT JOIN 
            product_pricing pp ON pp.product_id = p.id
        WHERE 
            p.deleted_at IS NULL 
            AND p.is_assured = FALSE 
            AND p.is_chronic = TRUE 
            AND p.is_active = true
            and p.is_discontinued = false
            AND p.mis_reporting_category IN ('Branded medicine', 'Branded Direct medicine', 'Branded OTC')
            AND pu.content in ('TABLET','CAPSULE')
    ),
    assured AS (
        SELECT 
            p.product_name AS a_product_name,
            p.product_code AS a_product_code, 
            p.mrp AS a_mrp, 
            p.is_assured AS a_is_assured, 
            p.is_rx_required AS a_is_rx_required, 
            p.is_chronic AS a_is_chronic, 
            p.is_refrigerated AS a_is_refrigerated, 
            p.mis_reporting_category AS a_catg, 
            pu.content AS a_dosage_form, 
            pu.package_type AS a_package_type, 
            pu.uom AS a_uom, 
            pu.package_size AS a_package_size,
            p.combinations_string AS a_combinations_string,
            pp.sales_price AS a_sales_price
        FROM 
            products p
        LEFT JOIN 
            product_units pu ON pu.product_id = p.id
        LEFT JOIN 
            product_pricing pp ON pp.product_id = p.id
        WHERE 
            p.deleted_at IS NULL 
            AND p.is_assured = TRUE 
            AND p.is_active = true
            and p.is_discontinued = false
            and pp.sales_price > 0 
            AND pu.content in ('TABLET','CAPSULE')
    )
    SELECT 
        bc.*,
        a.*
    FROM 
        branded_chronic bc
    JOIN 
        assured a 
    ON 
        bc.bc_combinations_string = a.a_combinations_string;
    '''


REPEAT_CUSTOMER_QUERY = '''
    SELECT 
        c.id AS customer_id,
        c.customer_code,
        c.full_name AS customer_name,
        c.mobile_number AS mobile_number,
        c.no_of_bills,
        c.ltv,
        c.loyalty_points,
        c.last_purchase_bill_date,
        stores.name as last_purchase_store_name,
        case when upper(stores.state)='GUJARAT' then 'GUJARATI' else 'HINDI' END as language      
    FROM customers c 
    INNER JOIN stores ON c.last_purchase_store_id = stores.id
    WHERE 
        stores.store_type = 'COCO' 
        AND (
            c.last_purchase_bill_date + INTERVAL '27 days' = CURRENT_DATE
            OR c.last_purchase_bill_date + INTERVAL '57 days' = CURRENT_DATE
            OR c.last_purchase_bill_date + INTERVAL '87 days' = CURRENT_DATE);
    '''

SALES_REPEAT_QUERY = '''
    SELECT
        si.store_id,
        TO_CHAR(si.created_at::date, 'YYYY-MM-DD') AS "billdate",
        si.customer_id as customer_id,
        si.id as sales_invoice_id,
        sid.product_id,
        p.ws_code as product_code,
        sid.quantity AS "Quantity",
        sid.bill_amount AS "Amount"
    FROM sales_invoice_details sid
    LEFT JOIN sales_invoices si ON sid.sales_invoice_id = si.id
    LEFT JOIN stores s ON si.store_id = s.id
    LEFT JOIN products p ON sid.product_id = p.id
    WHERE 
        sid.deleted_at IS NULL
        AND si.created_at::date >= NOW()-  INTERVAL '90 days'  
    '''

LOST_CUSTOMER_QUERY = '''
    SELECT
        c.id AS customer_id,
        c.customer_code,
        c.full_name AS customer_name,
        c.mobile_number AS mobile_number,
        c.no_of_bills,
        c.ltv,
        c.loyalty_points,
        c.last_purchase_bill_date,
        CASE 
            WHEN LOWER(stores.state) = 'gujarat' THEN 'GUJARATI' 
            ELSE 'HINDI' 
        END AS language
    FROM customers c 
    INNER JOIN stores ON c.last_purchase_store_id = stores.id
    WHERE 
        stores.store_type = 'COCO' 
        AND (c.last_purchase_bill_date + INTERVAL '6 months' >= CURRENT_DATE
            AND c.last_purchase_bill_date + INTERVAL '3 months' < CURRENT_DATE);
    '''

PRODUCT_MAPPED_DATA = '''
    select 
        ws_code,
        id 
    from products 
    where 
        is_banned is false 
        and is_discontinued is false 
        and content in ('TABLET','CAPSULE');
'''

def update_customer_campaign(successful_id):
    query = f'''
        UPDATE customer_campaigns
            SET is_message_sent = true,
            updated_at = CURRENT_TIMESTAMP
        WHERE 
            is_message_sent IS false
            AND id IN ({successful_id});
    '''

    return query



def get_customer_campaign_data(round):
    query = f'''
        select 
            id, 
            customer_mobile,
            customer_code,
            campaign_type, 
            language, 
            json_data, 
            round,
            campaign,
            savings_url
        from customer_campaigns 
        where 
            round = {round} 
            and is_message_sent is false;
    '''

    return query

def get_lost_customer_sales_query(customer_ids: tuple = (-1,), reference_date: datetime.date = None) -> str:

    if reference_date is None:
        reference_date = datetime.datetime.today().date()

    query = f"""
        SELECT
            si.store_id,
            si.created_at::date AS billdate,
            si.customer_id,
            si.id AS sales_invoice_id,
            sid.product_id,
            p.ws_code AS product_code,
            sid.quantity AS "Quantity",
            sid.bill_amount AS "Amount"
        FROM sales_invoice_details sid
        LEFT JOIN sales_invoices si ON sid.sales_invoice_id = si.id
        LEFT JOIN stores s ON si.store_id = s.id
        LEFT JOIN products p ON sid.product_id = p.id
        WHERE 
            sid.deleted_at IS NULL
            AND si.customer_id IN {tuple(customer_ids) if len(customer_ids) > 1 else f"({customer_ids[0]})"}
            AND si.created_at::date BETWEEN 
                TIMESTAMP '{reference_date}' - INTERVAL '8 months' 
                AND TIMESTAMP '{reference_date}' - INTERVAL '4 months 15 days';
    """

    return query


def get_repeat_customer_sales_query(customer_ids: list) -> str:

    formatted_customer_ids = tuple(customer_ids) if len(customer_ids) > 1 else f"({customer_ids[0]})"
    
    return f'''
        SELECT
            si.store_id,
            TO_CHAR(si.created_at::date, 'YYYY-MM-DD') AS "billdate",
            si.customer_id as customer_id,
            si.id as sales_invoice_id,
            sid.product_id,
            p.ws_code as product_code,
            sid.quantity AS "Quantity",
            sid.bill_amount AS "Amount"
        FROM sales_invoice_details sid
        LEFT JOIN sales_invoices si ON sid.sales_invoice_id = si.id
        LEFT JOIN stores s ON si.store_id = s.id
        LEFT JOIN products p ON sid.product_id = p.id
        WHERE 
            sid.deleted_at IS NULL
            AND si.customer_id IN {formatted_customer_ids}
            AND si.created_at::date >= NOW()-  INTERVAL '90 days'  
        '''

FIRST_FIVE_BILLS_CUSTOMER_QUERY = '''
    SELECT 
        c.id AS customer_id,
        c.customer_code,
        c.full_name AS customer_name,
        c.mobile_number AS mobile_number,
        c.no_of_bills,
        c.ltv,
        c.loyalty_points,
        c.last_purchase_bill_date,
        stores.name AS last_purchase_store_name,
        CASE 
            WHEN UPPER(stores.state) = 'GUJARAT' THEN 'GUJARATI' 
            ELSE 'HINDI' 
        END AS language,
        stores.city,
        CASE 
            WHEN c.last_purchase_bill_date + INTERVAL '27 days' = CURRENT_DATE THEN 'MSP'
            WHEN c.last_purchase_bill_date + INTERVAL '42 days' = CURRENT_DATE AND c.no_of_bills < 3 THEN 'MSP'
            WHEN c.last_purchase_bill_date + INTERVAL '42 days' = CURRENT_DATE AND c.no_of_bills > 2 THEN 'FREE_OTC'
            WHEN c.last_purchase_bill_date + INTERVAL '57 days' = CURRENT_DATE AND c.no_of_bills > 3 THEN '25_RUPEES'
            ELSE '0' 
        END AS campaign_type
    FROM customers c 
    INNER JOIN stores ON c.last_purchase_store_id = stores.id
    WHERE 
        stores.store_type = 'COCO' 
        AND c.no_of_bills < 5 
        AND c.aov > 100
        and c.last_purchase_bill_date::date >= '2025-01-01'
        AND 
        ( c.last_purchase_bill_date + INTERVAL '27 days' = CURRENT_DATE
            OR c.last_purchase_bill_date + INTERVAL '42 days' = CURRENT_DATE
            OR c.last_purchase_bill_date + INTERVAL '57 days' = CURRENT_DATE
        ) 
    '''

INSERT_GIFT_VOUCHER_STORE_QUERY = '''
    SELECT 
        id 
    FROM stores
    WHERE 
        store_type ='COCO' 
        AND is_active= True 
        AND is_pos_applicable = True 
        AND id <>24 ; 
    '''


INSERT_GIFT_VOUCHER_STORE_QUERY = '''
    SELECT 
        id 
    FROM stores
    WHERE 
        store_type ='COCO' 
        AND is_active= True 
        AND is_pos_applicable = True 
        AND id <>24 ; 
    '''

