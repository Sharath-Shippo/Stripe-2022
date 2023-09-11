DROP TABLE IF EXISTS console_query_variables;
CREATE TEMP TABLE console_query_variables AS
SELECT
    -- Date filter for whole query --> What dates range are we looking to pull?
    '2022-01-01 00:00:00'::timestamp AS query_start_timestamp_filter,
    '2023-02-01 00:00:00'::timestamp AS query_end_timestamp_filter,
    -- Date filter for source tables --> Given the above date range, what date ranges
    -- on the source tables should we look through?
    '2021-01-01 00:00:00'::timestamp AS source_tables_start_timestamp_filter,
    '2023-07-01 00:00:00'::timestamp AS source_tables_end_timestamp_filter,
    -16 AS fuzzymatchmin,     --DAYS
    6 AS fuzzymatchmax       --MONTHS
;
DROP TABLE IF EXISTS useroverride_detail;
CREATE TEMP TABLE useroverride_detail AS
    -- LOGIC PROVIDED BY DATA ENG TEAM (CALVIN)
SELECT
    uo2.id,
    uo2.object_owner_id,
    uo2.company_name_override,
    uo2.user_type_id,
    uo2.user_type_name,
    uo2.partner_platform
FROM
    (SELECT
         uo.id,
         uo.object_owner_id,
         uo.company_name_override,
         uo.user_type_id,
         t.name AS user_type_name,
         uo.partner_platform,
         RANK() OVER (PARTITION BY uo.object_owner_id ORDER BY uo.id DESC) rank_num
    FROM audit_data_se01_20230327.user_useroverride uo
    LEFT JOIN audit_data_se01_20230327.user_usertype t
    ON uo.user_type_id=t.id) uo2
WHERE uo2.rank_num = 1
--LIMIT 100
;




--Prod Invoices---
drop table if exists  Prod_Invoiceitem;
create temp table  Prod_Invoiceitem as
Select distinct invoice_id,paid,object_owner_id,sum(invcharge) as invcharge,sum(invrefund) as invrefund,
                sum(converted_invcharge)::numeric(15,2) as converted_invcharge,sum(converted_invrefund)::numeric(15,2) as converted_invrefund from
 (
Select distinct invoice_id,paid,invi.object_updated,invi.object_owner_id,
                max(to_char( CASE
                       WHEN invi.amount != 0
                           OR invi.amount IS NOT NULL THEN invi.object_updated
                       ELSE NULL
                       END,'YYYY-MM-DD HH24:MI')) as invoice_date,
    sum(
                   CASE
                       WHEN invi.object_purpose != 'REFUND'
                           AND (
                                        invi.amount != 0
                                    OR invi.amount IS NOT NULL
                                ) THEN invi.amount
                       ELSE NULL
                       END
               ) AS invcharge,
    sum(
                   CASE
                       WHEN invi.object_purpose = 'REFUND'
                           AND (
                                        invi.amount != 0
                                    OR invi.amount IS NOT NULL
                                ) THEN (-1 * invi.amount)
                       ELSE NULL
                       END
               ) AS invrefund,
     sum(
                   CASE
                       WHEN invi.object_purpose != 'REFUND'
                           AND (
                                        invi.amount != 0
                                    OR invi.amount IS NOT NULL
                                ) THEN (invi.amount)/MC.rate_type
                       ELSE NULL
                       END
               ) AS converted_invcharge,
    sum(
                   CASE
                       WHEN invi.object_purpose = 'REFUND'
                           AND (
                                        invi.amount != 0
                                    OR invi.amount IS NOT NULL
                                ) THEN (-1 * invi.amount)/MC.rate_type
                       ELSE NULL
                       END
               ) AS converted_invrefund
from audit_data_se01_20230327.api_invoiceitem invi
    left join audit_data_se01_20230327.api_invoice on invi.invoice_id = api_invoice.id
left join (Select * from api_currency) CUR on invi.currency_id=CUR.id
left join (Select distinct "Currency"::varchar(5) as ISO_Currnecy,Date::date as Currency_date,"Rate"::numeric(15,2) as rate_type
           from "22_audit_stripe_currency_2021_01_01_2023_07_12") MC
   on  CUR.iso=MC.ISO_Currnecy and invi.object_updated::date=MC.Currency_date::date
where  invi.object_updated>= (Select query_start_timestamp_filter from console_query_variables)
and invi.object_updated< (Select query_end_timestamp_filter from console_query_variables)
  and
        (
        invi.amount != 0
       OR invi.amount IS NOT NULL
       )
and invoice_id is not null
group by 1,2,3,4)A
where paid>=(Select query_start_timestamp_filter from console_query_variables) --Change 1
and paid <(Select query_end_timestamp_filter from console_query_variables)--Change 2
group by 1,2,3;


drop table if exists Billing_payment_info;
create temp table Billing_payment_info as
Select distinct invoice_id,
 object_created,
   object_updated,
   row_number () over (partition by  invoice_id order by object_created desc) as billing_row,
   sum(amount) as amount,
   sum(converted_amount)::numeric(15,2) as Amount_Converted
  from (Select distinct invoice_id,payment_method_id,
  currency_id,
 object_created,
  object_updated,
   amount,
  amount / rate_type as converted_amount
 from audit_data_se01_20230327.billing_payment BP
  left join (Select * from api_currency) CUR on BP.currency_id=CUR.id
left join (Select distinct "Currency"::varchar(5) as ISO_Currnecy,Date::date as Currency_date,"Rate"::numeric(15,2) as rate_type
           from "22_audit_stripe_currency_2021_01_01_2023_07_12") MC
   on  CUR.iso=MC.ISO_Currnecy and BP.object_updated::date=MC.Currency_date::date
 where object_created >= (Select query_start_timestamp_filter from console_query_variables)
 and object_created < (Select query_end_timestamp_filter from console_query_variables)
 and payment_method_id not in (3)
  )A
group by 1, 2, 3;

drop table if exists Billing_payment_Braintree;
create temp table Billing_payment_Braintree as
Select distinct invoice_id,
 object_created,
   object_updated,
   row_number () over (partition by  invoice_id order by object_created desc) as billing_row,
   sum(amount) as amount,
   sum(converted_amount)::numeric(15,2) as Amount_Converted
  from (Select distinct invoice_id,payment_method_id,
  currency_id,
 object_created,
  object_updated,
   amount,
  amount / rate_type as converted_amount
 from audit_data_se01_20230327.billing_payment BP
  left join (Select * from api_currency) CUR on BP.currency_id=CUR.id
left join (Select distinct "Currency"::varchar(5) as ISO_Currnecy,Date::date as Currency_date,"Rate"::numeric(15,2) as rate_type
           from "22_audit_stripe_currency_2021_01_01_2023_07_12") MC
   on  CUR.iso=MC.ISO_Currnecy and BP.object_updated::date=MC.Currency_date::date
 where object_created >= (Select query_start_timestamp_filter from console_query_variables)
 and object_created < (Select query_end_timestamp_filter from console_query_variables)
 and payment_method_id  in (3)
  )A
group by 1, 2, 3;

drop table if exists Stripe_data;
create temp table  Stripe_data as
Select   distinct A.balance_transaction_id,A.description,A.reporting_Category,created_utc,available_on_utc,B.invoice_id,
                   TO_CHAR(TO_DATE(created_utc, 'YYYY-MM-DD'), 'Mon-YY') as Stripe_month,
                 case when A.description like '%retry%' or A.description like '%reattempt%' and  A.reporting_category
                                                                 in ('charge') or  (A.description like '%-%' and A.reporting_category in ('charge','refund')) then 1 else 0 end as Invoices_retry,
                case when (length(B.invoice_id::text)<=4 and length(B.invoice_id::text)>0 and  A.reporting_category in ('charge','refund'))  then 1 else 0 end as net_suite,
        A.Gross,
         A.net,
        A.customer_facing_amount
 from "22_audit_stripe_transaction_history_recon_2022_01_2022_12_no_errors" A --Change the table
left join (SELECT  description,reporting_Category,REGEXP_REPLACE(description, '[^0-9]+', '') AS invoice_id,
               customer_facing_amount,gross
 from "22_audit_stripe_transaction_history_recon_2022_01_2022_12_no_errors" --Change the table
 where reporting_category in ('charge', 'refund', 'charge_failure')
 ) B
 on A.description = B.description
 --where A.reporting_category in ('charge', 'refund', 'charge_failure')
 union
Select   distinct A.balance_transaction_id,A.description,A.reporting_Category,created_utc,available_on_utc,B.invoice_id,
                   TO_CHAR(TO_DATE(created_utc, 'YYYY-MM-DD'), 'Mon-YY') as Stripe_month,
                 case when A.description like '%retry%' and  A.reporting_category
                                                                 in ('charge') or  (A.description   like '%-%' and A.reporting_category in ('charge','refund')) then 1 else 0 end as Invoices_retry,
                case when (length(B.invoice_id::text)<=4 and length(B.invoice_id::text)>0 and  A.reporting_category in ('charge','refund'))  then 1 else 0 end as net_suite,
        A.Gross,
         A.net,
        A.customer_facing_amount
 from "22_audit_stripe_transaction_history_recon_2022_01_2022_12_no_errors" A --Change the table
left join (SELECT  description,reporting_Category,REGEXP_REPLACE(description, '[^0-9]+', '') AS invoice_id,
               customer_facing_amount,gross
 from "22_audit_stripe_transaction_history_recon_2023_01_2023_06_no_errors" --Change the table
 where reporting_category in ('charge', 'refund', 'charge_failure')
 and file_name in ('Itemized_balance_change_from_activity_USD_2023-01-01_to_2023-01-31.csv')
 ) B
 on A.description = B.description
 --where A.reporting_category in ('charge', 'refund', 'charge_failure')
    where file_name in ('Itemized_balance_change_from_activity_USD_2023-01-01_to_2023-01-31.csv')
union
    Select   distinct A.balance_transaction_id,A.description,A.reporting_Category,created_utc,available_on_utc,B.invoice_id,
                   TO_CHAR(TO_DATE(created_utc, 'YYYY-MM-DD'), 'Mon-YY') as Stripe_month,
                 case when A.description like '%retry%' or A.description like '%reattempt%' and  A.reporting_category
                                                                 in ('charge') or  (A.description like '%-%' and A.reporting_category in ('charge','refund')) then 1 else 0 end as Invoices_retry,
                case when (length(B.invoice_id::text)<=4 and length(B.invoice_id::text)>0 and  A.reporting_category in ('charge','refund'))  then 1 else 0 end as net_suite,
        A.Gross,
         A.net,
        A.customer_facing_amount
 from "22_audit_stripe_transaction_history_recon_2021_10_2021_12_no_errors" A --Change the table
left join (SELECT  description,reporting_Category,REGEXP_REPLACE(description, '[^0-9]+', '') AS invoice_id,
               customer_facing_amount,gross
 from "22_audit_stripe_transaction_history_recon_2022_01_2022_12_no_errors" --Change the table
 where reporting_category in ('charge', 'refund', 'charge_failure')
 ) B
 on A.description = B.description;
 --where A.reporting_category in ('charge', 'refund', 'charge_failure')

/**Invoice_base as (
Select distinct invoice_id from
(Select distinct invoice_id from stripe_data where net_suite=0)A
union
(Select distinct invoice_id from Prod_Invoiceitem)
union
(Select distinct invoice_id from Billing_payment_info)
),**/


/**invoice_date_info as (
Select distinct IB.invoice_id,
    BP.object_Created as date_info_billing_date,
    max(to_char(CASE
   WHEN invi.amount != 0
     OR invi.amount IS NOT NULL THEN invi.object_updated
    ELSE NULL
   END, 'YYYY-MM-DD HH24:MI')) as date_info_invoice_date
   from Invoice_base IB
 inner join
   (Select distinct invoice_id,object_updated,amount from api_invoiceitem  ) INVI on IB.invoice_id = INVI.invoice_id
   inner join (Select distinct invoice_id, object_Created from public.billing_payment where  payment_method_id not in (3)  ) BP
  on IB.invoice_id = BP.invoice_id
 group by 1, 2)**/


/*Invoice_having_pre2019 as (
Select distinct STRP.invoice_id,First_Created_Date,case when First_Created_Date::date<='2019-01-01' then 1 else 0 end as "Before_2019"
from Stripe_data STRP
    inner join (Select distinct invoice_id,min(to_char( CASE
                       WHEN invi.amount != 0
                           OR invi.amount IS NOT NULL THEN invi.object_created
                       ELSE NULL
                       END,'YYYY-MM-DD HH24:MI')) as First_Created_Date from api_invoiceitem invi
group by 1
)A on STRP.invoice_id=A.invoice_id)*/

/**
later_than_updated as (
Select distinct STRP.invoice_id, case when Prod_updated_date::date > Billing_created_date then 1 else 0 end as later_than_updated
from Stripe_data STRP
   left join  (Select distinct invoice_id,max(to_char( CASE
                       WHEN invi.amount != 0
                           OR invi.amount IS NOT NULL THEN invi.object_updated
                       ELSE NULL
                       END,'YYYY-MM-DD HH24:MI')) as Prod_updated_date from api_invoiceitem invi where  invoice_id=1969107 group by 1) inv on STRP.invoice_id=inv.invoice_id
    left join (Select distinct invoice_id,object_created as Billing_created_date
    from billing_payment
    where object_created>=TO_DATE(current_setting('ending.Billing.tblfilter'), 'YYYY-MM-DD')
    and invoice_id=1969107 ) BP on STRP.invoice_id=BP.invoice_id)**/

drop table if exists Invoice_check;
create temp table Invoice_check as
    Select distinct id,paid as invoice_paid,object_created as invoice_created from api_invoice
    where id in (Select distinct invoice_id from Stripe_data where net_suite=0 and Invoices_retry=0 and reporting_category in ('charge','refund'))
;
drop table if exists Billing_check;
create temp table Billing_check as
    Select distinct invoice_id,object_created as billing_created,amount  from billing_payment
    where id in (Select distinct invoice_id from Stripe_data where net_suite=0 and Invoices_retry=0 and reporting_category in ('charge','refund'))
;

drop table if exists Stripe_Summary;
create temp table Stripe_Summary as
Select Month, Category,reporting_Category,invoice_id,description,STRP_flag,INV_flag,Billing_flag,Braintree_flag,Before_2021,paid_before,pre_auth,Invoices_retry,net_suite,object_owner_id,
       sum(customer_facing_amount)customer_facing_amount,sum(gross) as gross,sum(net) net,sum(USD_invcharge) as USD_invcharge,sum(USD_invrefund) as USD_invrefund,sum(USD_BP_Amount) as BIlling_Amount,
       sum(USD_BT_Amount) as Braintree_Amount
from
    (Select   Stripe_month as Month,'Stripe-Prod' as Category,STRP.reporting_Category,description,created_utc,available_on_utc,STRP.invoice_id, --Change 7
    case when STRP.invoice_id is not null then 1 else 0 end as STRP_flag,
    case when INV.invoice_id is not null then 1 else 0 end as INV_flag,
     case when BP.invoice_id is not null then 1 else 0 end as Billing_flag,
      case when BT.invoice_id is not null then 1 else 0 end as Braintree_flag,object_owner_id,
      Case when invoice_created::date < (select source_tables_start_timestamp_filter from console_query_variables) then 1 else 0 end as Before_2021,
     Case when invoice_paid::date < (select query_start_timestamp_filter from console_query_variables)  then 1 else 0 end as paid_before,
     Case when description like'%Preauthorization hold for first label purchase%' then 1 else 0 end as pre_auth,
     case when INV.paid is null then invoice_paid else INV.paid end  as invoice_date,
     case when BP.object_created is null then billing_created else BP.object_created end  as Billing_date,
   Invoices_retry,net_suite,
    sum(customer_facing_amount) as customer_facing_amount,sum(gross) as gross,sum(net) as net,
                sum(invcharge) as invcharge,sum(invrefund) as invrefund,
                sum(converted_invcharge) as USD_invcharge,sum(converted_invrefund) as USD_invrefund,
                sum(BP.Amount_Converted) as USD_BP_Amount,sum(BT.Amount_Converted) as USD_BT_Amount
from           Stripe_data    STRP
    --left join invoice_date_info ID on STRP.invoice_id=ID.invoice_id
    --left join Invoice_having_pre2019 PRE on STRP.invoice_id=PRE.invoice_id
    --left join later_than_updated UP on STRP.invoice_id=UP.invoice_id
left join  Prod_Invoiceitem  INV on STRP.invoice_id=INV.invoice_id
    left join Invoice_check IC on STRP.invoice_id=IC.id
    left join Billing_check BC on STRP.invoice_id=BC.invoice_id
   left join (Select distinct invoice_id,object_created,object_updated,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_Braintree where billing_row=1 group by 1,2,3) BT on STRP.invoice_id=BT.invoice_id
left join (Select distinct invoice_id,object_created,object_updated,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_info where billing_row=1 group by 1,2,3) BP on STRP.invoice_id=BP.invoice_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)A
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
union
Select Month, Category,reporting_Category,invoice_id,description,STRP_flag,INV_flag,Billing_flag,Braintree_flag,Before_2021,paid_before,pre_auth,Invoices_retry,net_suite,object_owner_id,
       sum(customer_facing_amount)customer_facing_amount,sum(gross) as gross,sum(net) net,sum(USD_invcharge) as USD_invcharge,sum(USD_invrefund) as USD_invrefund,sum(USD_BP_Amount) as BIlling_Amount,
       sum(USD_BT_Amount) as Braintree_Amount
from (Select TO_CHAR(TO_DATE(paid, 'YYYY-MM-DD'), 'Mon-YY') as Month ,'Prod-Stripe' as Category,'' as reporting_Category,'' as description,
        created_utc,  available_on_utc,cast(INV.invoice_id as varchar(256)) as invoice_id, --Change 8
    case when STRP.invoice_id is not null then 1 else 0 end as STRP_flag,
    case when INV.invoice_id is not null then 1 else 0 end as INV_flag,
    case when BP.invoice_id is not null then 1 else 0 end as Billing_flag,
    case when BT.invoice_id is not null then 1 else 0 end as Braintree_flag,object_owner_id,
     Case when invoice_created::date < (select source_tables_start_timestamp_filter from console_query_variables) then 1 else 0 end as Before_2021,
     Case when invoice_paid::date < (select query_start_timestamp_filter from console_query_variables)  then 1 else 0 end as paid_before,
     Case when description like'%Preauthorization hold for first label purchase%' then 1 else 0 end as pre_auth,
    case when INV.paid is null then invoice_paid else INV.paid end  as invoice_date,
     case when BP.object_created is null then billing_created else INV.paid end  as Billing_date,
     0 as Invoices_retry, 0 as net_suite,
    sum(customer_facing_amount) as customer_facing_amount,sum(gross) as gross,sum(net) as net,
                sum(invcharge) as invcharge,sum(invrefund) as invrefund,
                sum(converted_invcharge) as USD_invcharge,sum(converted_invrefund) as USD_invrefund,
                sum(BP.Amount_Converted) as USD_BP_Amount,sum(BT.Amount_Converted) as USD_BT_Amount
from       Prod_Invoiceitem INV
     left join Invoice_check IC on INV.invoice_id=IC.id
    left join Billing_check BC on INV.invoice_id=BC.invoice_id
        left join (Select distinct invoice_id,object_created,object_updated,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_Braintree where billing_row=1 group by 1,2,3) BT on INV.invoice_id=BT.invoice_id
    --left join invoice_date_info ID on INV.invoice_id=ID.invoice_id
    --left join Invoice_having_pre2019 PRE on INV.invoice_id=PRE.invoice_id
    --left join later_than_updated UP on INV.invoice_id=UP.invoice_id
left join  (Select * from (Select S.*,row_number () over (PARTITION BY invoice_id order by gross desc ) as Invoice_count from  Stripe_data S)A where Invoice_count=1)  STRP on INV.invoice_id=STRP.invoice_id
left join (Select distinct invoice_id,object_created,object_updated,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_info where billing_row=1 group by 1,2,3) BP on INV.invoice_id=BP.invoice_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)A
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15;

DROP TABLE IF EXISTS default_address;
CREATE TEMP TABLE default_address AS
SELECT DISTINCT
  -- Looker/DWH only pulls in address table data for when default_sender is true (per Calvin),
  -- so this table is just mirroring what is available in looker
  *
FROM audit_data_se01_20230327.api_apiaddress addr
       LEFT JOIN
   (SELECT object_owner_id     ooid,
           min(object_created) unique_create_date
    FROM audit_data_se01_20230327.api_apiaddress
    WHERE is_default_sender is TRUE
    GROUP BY object_owner_id
    ORDER BY object_owner_id) AS uooid
   ON addr.object_owner_id = uooid.ooid AND addr.object_created = uooid.unique_create_date
WHERE (uooid.ooid is NOT NULL AND uooid.unique_create_date is NOT NULL)
 AND is_default_sender is TRUE
 --AND addr.object_owner_id in ('1400689')
 --AND (object_created >= (select source_tables_start_timestamp_filter from query_variables)
 --AND object_created < (select source_tables_end_timestamp_filter from query_variables))
 --limit 10
;

DROP TABLE IF EXISTS transaction_detail;
CREATE TEMP TABLE transaction_detail AS
SELECT DISTINCT
      -- Pulls required data from api_transaction and direct joins the following tables
      -- api_rate, track_status, auth_user, api_refund, user_useroverride_tmp

      -- Api_transaction
      txn.id                                       api_transaction_id,
      txn.cost_usd                                 est_postage_cost,
      txn.amount                                   actual_postage_cost,
      txn.was_test,
      txn.object_state                             api_transaction_object_state,
      txn.object_status                            api_transaction_object_status,
      txn.tracking_number,
      txn.entry_point,
      txn.scan_form_id,
      txn.track_status_id,
      txn.api_rate_id,
      txn.object_created                           api_transaction_object_created,
      txn.object_updated                           api_transaction_object_updated,
      txn.object_owner_id                          api_transaction_object_owner,
      r8.amount                                    user_rate,
      r8.insurance_amount,
      r8.amount_insurance_fee                      insurance_fee,
      r8.insurance_cost,
      r8.zone,
      r8.servicelevel_id,
      r8.servicelevel_name,
      r8.provider_id,
      r8.shipment_id,
      r8.account_id,
      ts.name                                      track_status_name,
      au.id                                        au_id,
      au.apiuser_id                                api_user_id,
      au.username,
      au.platform_id,
      uuo.company_name_override,
      ref.object_status                            api_refund_object_status,
      ref.carrier_status                           api_refund_carrier_status,
      ref.approve_time                             api_refund_approve_time,

      -- Discount group name - should be replaced with billing_discount_group table
      -- Once Data Eng adds it to the Audit DB
      CASE
          WHEN r8.discount_group_id = 1
              THEN 'EBAY_CA'
          WHEN r8.discount_group_id = 2
              THEN 'EBAY_US'
          WHEN r8.discount_group_id = 3
              THEN 'ES_SHIPPO_REFERRED'
          WHEN r8.discount_group_id = 4
              THEN 'ES_EXPRESS_SAVE_REFERRED'
          ELSE 'NOT SET'
          END                                   AS discount_group_name,

      -- Scanbased_outbound_indicator logic from Hong-Kit, 20221129 part of transform logic from DWH to OLTP
      CASE
          WHEN r8.provider_id in
               (
                5, -- FedEx
                50, -- Hermes UK
                56, -- DPD UK
                95, -- Colissimo
                96, -- DPD DE
                98, -- Poste Italiane
                38, -- Mondial Relay
                41, -- OnTrac
                28, -- Correos
                40 -- LaserShip
                   )
              THEN 'Scan-Based Outbound'
          ELSE 'Not Scan-Based Outbound'
          END                                   as scanbased_outbound_indicator,

      -- Label and Refund dates formatted
      to_char(txn.object_created, 'MON-YY')     AS lbl_generation_mnth,

      to_char(ref.approve_time, 'MON-YY')       AS refund_date_mnth,

      to_char(txn.object_created, 'YYYY-MM-DD') AS lbl_generation_date,

      to_char(ref.approve_time, 'YYYY-MM-DD')   AS refund_approval_time,

      to_char(txn.object_updated, 'YYYY-MM-DD') AS trx_updated

  FROM audit_data_se01_20230327.api_transaction txn
           LEFT JOIN audit_data_se01_20230327.api_refund ref ON txn.id = ref.transaction_id
      AND (ref.object_created >= (select source_tables_start_timestamp_filter from console_query_variables)
          AND ref.object_created < (select source_tables_end_timestamp_filter from console_query_variables))
           LEFT JOIN audit_data_se01_20230327.track_status ts ON txn.track_status_id = ts.id
           LEFT JOIN audit_data_se01_20230327.api_rate r8 ON txn.api_rate_id = r8.id
      AND (r8.object_created >= (select source_tables_start_timestamp_filter from console_query_variables)
          AND r8.object_created < (select source_tables_end_timestamp_filter from console_query_variables))
           LEFT JOIN audit_data_se01_20230327.auth_user au ON txn.object_owner_id = au.id
           LEFT JOIN useroverride_detail uuo ON txn.object_owner_id = uuo.object_owner_id

  WHERE (txn.object_created >= (select source_tables_start_timestamp_filter from console_query_variables)
      AND txn.object_created < (select source_tables_end_timestamp_filter from console_query_variables))
    AND au.id NOT IN ('1206048')
    AND txn.object_state = 'VALID'
    AND txn.was_test = 'false'
    -- oltp transform logic from Hong-Kit 20221130
    AND txn.object_status NOT IN ('ERROR', 'WAITING', 'QUEUED')
    -- diagnostic/ filter logic
   AND r8.provider_id = '17'
    --AND txn.object_owner_id in ('')
--        AND txn.id in
--            (
--'750448009',
--'750460534',
--'750679110',
--'750557676',
--'750725632'
--         )

  ORDER BY txn.id
  -- limit 10
;

Select Month, Category,reporting_Category,STRP_flag,INV_flag,Billing_flag,Braintree_flag,Before_2021,paid_before,pre_auth,Invoices_retry,net_suite,count(distinct invoice_id) invoices,
       sum(customer_facing_amount)customer_facing_amount,sum(gross) as gross,sum(net) net,sum(USD_invcharge) as USD_invcharge,sum(USD_invrefund) as USD_invrefund,sum(USD_BP_Amount) as BIlling_Amount,sum(USD_BT_Amount) as Braintree_Amount
from
    (Select   Stripe_month as Month,'Stripe-Prod' as Category,STRP.reporting_Category,description,created_utc,available_on_utc,STRP.invoice_id, --Change 7
    case when STRP.invoice_id is not null then 1 else 0 end as STRP_flag,
    case when INV.invoice_id is not null then 1 else 0 end as INV_flag,
     case when BP.invoice_id is not null then 1 else 0 end as Billing_flag,
     case when BT.invoice_id is not null then 1 else 0 end as Braintree_flag,
      Case when invoice_created::date < (select source_tables_start_timestamp_filter from console_query_variables) then 1 else 0 end as Before_2021,
     Case when invoice_paid::date < (select query_start_timestamp_filter from console_query_variables)  then 1 else 0 end as paid_before,
     Case when description like'%Preauthorization hold for first label purchase%' then 1 else 0 end as pre_auth,
     case when INV.paid is null then invoice_paid else INV.paid end  as invoice_date,
     case when BP.object_created is null then billing_created else BP.object_created end  as Billing_date,
   Invoices_retry,net_suite,
    sum(customer_facing_amount) as customer_facing_amount,sum(gross) as gross,sum(net) as net,
                sum(invcharge) as invcharge,sum(invrefund) as invrefund,
                sum(converted_invcharge) as USD_invcharge,sum(converted_invrefund) as USD_invrefund,
                sum(BP.Amount_Converted) as USD_BP_Amount,sum(BT.Amount_Converted) as USD_BT_Amount
from           Stripe_data    STRP
    --left join invoice_date_info ID on STRP.invoice_id=ID.invoice_id
    --left join Invoice_having_pre2019 PRE on STRP.invoice_id=PRE.invoice_id
    --left join later_than_updated UP on STRP.invoice_id=UP.invoice_id
left join  Prod_Invoiceitem  INV on STRP.invoice_id=INV.invoice_id
    left join Invoice_check IC on STRP.invoice_id=IC.id
    left join Billing_check BC on STRP.invoice_id=BC.invoice_id
   left join (Select distinct invoice_id,object_created,object_updated,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_Braintree where billing_row=1 group by 1,2,3) BT on STRP.invoice_id=BT.invoice_id
left join (Select distinct invoice_id,object_created,object_updated,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_info where billing_row=1 group by 1,2,3) BP on STRP.invoice_id=BP.invoice_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)A
group by 1,2,3,4,5,6,7,8,9,10,11,12
union
Select Month, Category,reporting_Category,STRP_flag,INV_flag,Billing_flag,Braintree_flag,Before_2021,paid_before,pre_auth,Invoices_retry,net_suite,count(distinct invoice_id) invoices,
       sum(customer_facing_amount)customer_facing_amount,sum(gross) as gross,sum(net) net,sum(USD_invcharge) as USD_invcharge,sum(USD_invrefund) as USD_invrefund,sum(USD_BP_Amount) as BIlling_Amount,sum(USD_BT_Amount) as Braintree_Amount
from (Select TO_CHAR(TO_DATE(paid, 'YYYY-MM-DD'), 'Mon-YY') as Month ,'Prod-Stripe' as Category,'' as reporting_Category,'' as description,
        created_utc,  available_on_utc,cast(INV.invoice_id as varchar(256)) as invoice_id, --Change 8
    case when STRP.invoice_id is not null then 1 else 0 end as STRP_flag,
    case when INV.invoice_id is not null then 1 else 0 end as INV_flag,
    case when BP.invoice_id is not null then 1 else 0 end as Billing_flag,
    case when BT.invoice_id is not null then 1 else 0 end as Braintree_flag,
     Case when invoice_created::date < (select source_tables_start_timestamp_filter from console_query_variables) then 1 else 0 end as Before_2021,
     Case when invoice_paid::date < (select query_start_timestamp_filter from console_query_variables)  then 1 else 0 end as paid_before,
     Case when description like'%Preauthorization hold for first label purchase%' then 1 else 0 end as pre_auth,
    case when INV.paid is null then invoice_paid else INV.paid end  as invoice_date,
     case when BP.object_created is null then billing_created else INV.paid end  as Billing_date,
     0 as Invoices_retry, 0 as net_suite,
    sum(customer_facing_amount) as customer_facing_amount,sum(gross) as gross,sum(net) as net,
                sum(invcharge) as invcharge,sum(invrefund) as invrefund,
                sum(converted_invcharge) as USD_invcharge,sum(converted_invrefund) as USD_invrefund,
                sum(BP.Amount_Converted) as USD_BP_Amount,sum(BT.Amount_Converted) as USD_BT_Amount
from       Prod_Invoiceitem INV
     left join Invoice_check IC on INV.invoice_id=IC.id
    left join Billing_check BC on INV.invoice_id=BC.invoice_id
      left join (Select distinct invoice_id,object_created,object_updated,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_Braintree where billing_row=1 group by 1,2,3) BT on INV.invoice_id=BT.invoice_id
    --left join invoice_date_info ID on INV.invoice_id=ID.invoice_id
    --left join Invoice_having_pre2019 PRE on INV.invoice_id=PRE.invoice_id
    --left join later_than_updated UP on INV.invoice_id=UP.invoice_id
left join  (Select * from (Select S.*,row_number () over (PARTITION BY invoice_id order by gross desc ) as Invoice_count from  Stripe_data S)A where Invoice_count=1)  STRP on INV.invoice_id=STRP.invoice_id
left join (Select distinct invoice_id,object_created,object_updated,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_info where billing_row=1 group by 1,2,3) BP on INV.invoice_id=BP.invoice_id
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)A
group by 1,2,3,4,5,6,7,8,9,10,11,12;






/*--Check 1--Not available in Billing Payment
Select * from billing_payment
where invoice_id in (Select distinct  invoice_id from
(Select Month, Category,reporting_Category,STRP_flag,INV_flag,Billing_flag,Before_2021,paid_before,paid_after,Invoices_retry,net_suite,payment_method_id,invoice_id,
       sum(customer_facing_amount)customer_facing_amount,sum(gross) as gross,sum(net) net,sum(USD_invcharge) as USD_invcharge,sum(USD_invrefund) as USD_invrefund,sum(USD_BP_Amount) as BIlling_Amount
from (Select TO_CHAR(TO_DATE(paid, 'YYYY-MM-DD'), 'Mon-YY') as Month ,'Prod-Stripe' as Category,'' as reporting_Category,'' as description,payment_method_id,
        created_utc,  available_on_utc,cast(INV.invoice_id as varchar(256)) as invoice_id, --Change 8
    case when STRP.invoice_id is not null then 1 else 0 end as STRP_flag,
    case when INV.invoice_id is not null then 1 else 0 end as INV_flag,
    case when BP.invoice_id is not null then 1 else 0 end as Billing_flag,
     Case when invoice_created::date < (select source_tables_start_timestamp_filter from console_query_variables) then 1 else 0 end as Before_2021,
     Case when invoice_paid::date < (select query_start_timestamp_filter from console_query_variables)  then 1 else 0 end as paid_before,
     Case when invoice_paid::date >(select query_end_timestamp_filter from console_query_variables) then 1 else 0 end as paid_after,
    case when INV.paid is null then invoice_paid else INV.paid end  as invoice_date,
     case when BP.object_created is null then billing_created else INV.paid end  as Billing_date,
     0 as Invoices_retry, 0 as net_suite,
    sum(customer_facing_amount) as customer_facing_amount,sum(gross) as gross,sum(net) as net,
                sum(invcharge) as invcharge,sum(invrefund) as invrefund,
                sum(converted_invcharge) as USD_invcharge,sum(converted_invrefund) as USD_invrefund,
                sum(Amount_Converted) as USD_BP_Amount,sum(BP.amount) as BP_amount
from       Prod_Invoiceitem INV
     left join Invoice_check IC on INV.invoice_id=IC.id
    left join Billing_check BC on INV.invoice_id=BC.invoice_id
    --left join invoice_date_info ID on INV.invoice_id=ID.invoice_id
    --left join Invoice_having_pre2019 PRE on INV.invoice_id=PRE.invoice_id
    --left join later_than_updated UP on INV.invoice_id=UP.invoice_id
left join  (Select * from (Select S.*,row_number () over (PARTITION BY invoice_id order by gross desc ) as Invoice_count from  Stripe_data S)A where Invoice_count=1)  STRP on INV.invoice_id=STRP.invoice_id
left join (Select distinct invoice_id,object_created,object_updated,payment_method_id,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_info where billing_row=1 group by 1,2,3,4) BP on INV.invoice_id=BP.invoice_id
where STRP.invoice_id is null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)A
group by 1,2,3,4,5,6,7,8,9,10,11,12,13)A
where payment_method_id is null);*/


---Check 2--
/*invoice_item_check as
(Select * from api_invoiceitem
where invoice_id in (Select distinct  invoice_id from
(Select Month, Category,reporting_Category,STRP_flag,INV_flag,Billing_flag,Before_2021,paid_before,paid_after,Invoices_retry,net_suite,payment_method_id,invoice_id,
       sum(customer_facing_amount)customer_facing_amount,sum(gross) as gross,sum(net) net,sum(USD_invcharge) as USD_invcharge,sum(USD_invrefund) as USD_invrefund,sum(USD_BP_Amount) as BIlling_Amount
from (Select TO_CHAR(TO_DATE(paid, 'YYYY-MM-DD'), 'Mon-YY') as Month ,'Prod-Stripe' as Category,'' as reporting_Category,'' as description,payment_method_id,
        created_utc,  available_on_utc,cast(INV.invoice_id as varchar(256)) as invoice_id, --Change 8
    case when STRP.invoice_id is not null then 1 else 0 end as STRP_flag,
    case when INV.invoice_id is not null then 1 else 0 end as INV_flag,
    case when BP.invoice_id is not null then 1 else 0 end as Billing_flag,
     Case when invoice_created::date < (select source_tables_start_timestamp_filter from console_query_variables) then 1 else 0 end as Before_2021,
     Case when invoice_paid::date < (select query_start_timestamp_filter from console_query_variables)  then 1 else 0 end as paid_before,
     Case when invoice_paid::date >(select query_end_timestamp_filter from console_query_variables) then 1 else 0 end as paid_after,
    case when INV.paid is null then invoice_paid else INV.paid end  as invoice_date,
     case when BP.object_created is null then billing_created else INV.paid end  as Billing_date,
     0 as Invoices_retry, 0 as net_suite,
    sum(customer_facing_amount) as customer_facing_amount,sum(gross) as gross,sum(net) as net,
                sum(invcharge) as invcharge,sum(invrefund) as invrefund,
                sum(converted_invcharge) as USD_invcharge,sum(converted_invrefund) as USD_invrefund,
                sum(Amount_Converted) as USD_BP_Amount,sum(BP.amount) as BP_amount
from       Prod_Invoiceitem INV
     left join Invoice_check IC on INV.invoice_id=IC.id
    left join Billing_check BC on INV.invoice_id=BC.invoice_id
    --left join invoice_date_info ID on INV.invoice_id=ID.invoice_id
    --left join Invoice_having_pre2019 PRE on INV.invoice_id=PRE.invoice_id
    --left join later_than_updated UP on INV.invoice_id=UP.invoice_id
left join  (Select * from (Select S.*,row_number () over (PARTITION BY invoice_id order by gross desc ) as Invoice_count from  Stripe_data S)A where Invoice_count=1)  STRP on INV.invoice_id=STRP.invoice_id
left join (Select distinct invoice_id,object_created,object_updated,payment_method_id,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_info where billing_row=1 group by 1,2,3,4) BP on INV.invoice_id=BP.invoice_id
where STRP.invoice_id is null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)A
group by 1,2,3,4,5,6,7,8,9,10,11,12,13)A
where payment_method_id is null))


Select from api_transaction
where id in (Select distinct transaction_id from invoice_item_check);


invoice_item_check as
(Select * from Prod_Invoiceitem
where invoice_id in (Select distinct  invoice_id from
(Select Month, Category,reporting_Category,STRP_flag,INV_flag,Billing_flag,Before_2021,paid_before,paid_after,Invoices_retry,net_suite,payment_method_id,invoice_id,
       sum(customer_facing_amount)customer_facing_amount,sum(gross) as gross,sum(net) net,sum(USD_invcharge) as USD_invcharge,sum(USD_invrefund) as USD_invrefund,sum(USD_BP_Amount) as BIlling_Amount
from (Select TO_CHAR(TO_DATE(paid, 'YYYY-MM-DD'), 'Mon-YY') as Month ,'Prod-Stripe' as Category,'' as reporting_Category,'' as description,payment_method_id,
        created_utc,  available_on_utc,cast(INV.invoice_id as varchar(256)) as invoice_id, --Change 8
    case when STRP.invoice_id is not null then 1 else 0 end as STRP_flag,
    case when INV.invoice_id is not null then 1 else 0 end as INV_flag,
    case when BP.invoice_id is not null then 1 else 0 end as Billing_flag,
     Case when invoice_created::date < (select source_tables_start_timestamp_filter from console_query_variables) then 1 else 0 end as Before_2021,
     Case when invoice_paid::date < (select query_start_timestamp_filter from console_query_variables)  then 1 else 0 end as paid_before,
     Case when invoice_paid::date >(select query_end_timestamp_filter from console_query_variables) then 1 else 0 end as paid_after,
    case when INV.paid is null then invoice_paid else INV.paid end  as invoice_date,
     case when BP.object_created is null then billing_created else INV.paid end  as Billing_date,
     0 as Invoices_retry, 0 as net_suite,
    sum(customer_facing_amount) as customer_facing_amount,sum(gross) as gross,sum(net) as net,
                sum(invcharge) as invcharge,sum(invrefund) as invrefund,
                sum(converted_invcharge) as USD_invcharge,sum(converted_invrefund) as USD_invrefund,
                sum(Amount_Converted) as USD_BP_Amount,sum(BP.amount) as BP_amount
from       Prod_Invoiceitem INV
     left join Invoice_check IC on INV.invoice_id=IC.id
    left join Billing_check BC on INV.invoice_id=BC.invoice_id
    --left join invoice_date_info ID on INV.invoice_id=ID.invoice_id
    --left join Invoice_having_pre2019 PRE on INV.invoice_id=PRE.invoice_id
    --left join later_than_updated UP on INV.invoice_id=UP.invoice_id
left join  (Select * from (Select S.*,row_number () over (PARTITION BY invoice_id order by gross desc ) as Invoice_count from  Stripe_data S)A where Invoice_count=1)  STRP on INV.invoice_id=STRP.invoice_id
left join (Select distinct invoice_id,object_created,object_updated,payment_method_id,sum(Amount_Converted)as Amount_Converted,sum(amount) as amount from Billing_payment_info where billing_row=1 group by 1,2,3,4) BP on INV.invoice_id=BP.invoice_id
where STRP.invoice_id is null
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18)A
group by 1,2,3,4,5,6,7,8,9,10,11,12,13)A
where payment_method_id is null))

select distinct invoice_id,paid
from invoice_item_check group by 1

Select distinct txn.object_owner_id,company_name_override,partner_platform,entry_point from api_transaction txn
 inner join  audit_data_se01_20230327.auth_user au ON txn.object_owner_id = au.id
 LEFT JOIN useroverride_detail uuo ON txn.object_owner_id = uuo.object_owner_id
where txn.id in (Select distinct transaction_id from invoice_item_check);
