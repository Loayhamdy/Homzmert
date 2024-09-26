from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue import DynamicFrame
import boto3
import sys
import json


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


####################### Initiation start #####################
try:
     args = getResolvedOptions(sys.argv, ['JOB_NAME','WORKFLOW_NAME', 'WORKFLOW_RUN_ID'])
    
except:
     args = getResolvedOptions(sys.argv, ["JOB_NAME",'RUNNING_FROM','RUNNING_TO','SOURCE_SECRET_NAME','REDSHIFT_SECRET_NAME','REDSHIFT_TEMP'])
    
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


glue_client = boto3.client("glue")
sc_client = boto3.client("secretsmanager", region_name="eu-central-1")

try :
   
    workflow_name = args['WORKFLOW_NAME']
    workflow_run_id = args['WORKFLOW_RUN_ID']
    workflow_params = glue_client.get_workflow_run_properties(Name=workflow_name,
                                        RunId=workflow_run_id)["RunProperties"]
    period_from=workflow_params['RUNNING_FROM']
    period_to=workflow_params['RUNNING_TO']
    s_source=workflow_params['SOURCE_SECRET_NAME']
    s_redshift=workflow_params['REDSHIFT_SECRET_NAME']
    redshift_temp=workflow_params['REDSHIFT_TEMP']
except :
 
   period_from=args['RUNNING_FROM']
   period_to=args['RUNNING_TO']
   s_source=args['SOURCE_SECRET_NAME']
   s_redshift=args['REDSHIFT_SECRET_NAME']
   redshift_temp=args['REDSHIFT_TEMP']



source_secret_value_response = sc_client.get_secret_value(
        SecretId=s_source
)

redshift_secret_value_response = sc_client.get_secret_value(
        SecretId=s_redshift
)

secret_source = source_secret_value_response['SecretString']
secret_source = json.loads(secret_source)

secret_redshift = redshift_secret_value_response['SecretString']
secret_redshift = json.loads(secret_redshift)

db_username = secret_source.get('username')
db_password = secret_source.get('password')
db_host = secret_source.get('host')
db_port = secret_source.get('port')
db_dbname = secret_source.get('dbname')
db_engine = secret_source.get('engine')

redshift_username = secret_redshift.get('username')
redshift_password = secret_redshift.get('password')
redshift_host = secret_redshift.get('host')
redshift_port = secret_redshift.get('port')
redshift_dbname = secret_redshift.get('dbname')
redshift_schema = str(secret_redshift.get('schema'))
redshift_conn = secret_redshift.get('conn')
redshift_engine="redshift"

## Source Databce
url="jdbc:"+redshift_engine+"://"+redshift_host+":"+redshift_port+"/"+redshift_dbname


if   period_from =="current_date-1"  :
     cond_from=period_from
     cond_to=period_to
else : 
     cond_from="to_date('"+period_from+"','ddmmyyyy')"
     cond_to="to_date('"+period_to+"','ddmmyyyy')"

####################### Initiation End #####################

# Script generated for node SQL
SqlQuery0 = """
select
  sales_item_key,
  facts.order_key,
  item_created_at_dt creation_date,
  item_updated_at_ts,
  item_updated_at_dt,
  case
  when upper(facts.status) in ('DELIVERED', 'COMPLETE', 'RETURNED', 'CLOSED') then last_del_status_dt
  when upper(facts.status) = 'REJECTED' then last_rej_status_dt end operating_dt,
  last_confirm_status_dt,
  case
  when mobile_order = 1
  and ras.store_key is null then 'App'
  when mobile_order = 1
  and ras.store_key is not null then 'App_' + ras.store_name
  when mobile_order = 0
  and ras.store_key is null then 'Website'
  when mobile_order = 0
  and ras.store_key is not null then 'Website_' + ras.store_name end order_channel,
  facts.sla_max_days customer_sla,
  case
  when nvl(facts.sla_max_days, 0) > 0 then facts.sla_max_days -3
  else facts.sla_max_days end seller_sla,
  region_name,
  last_status,
  nvl(facts.price, 0) price,
  facts.sku,
  facts.url_key,
  facts.seller_key seller_id,
  seller_name_eg as seller_name,
  nvl(shipping_fees, 0) shipping_fees,
  nvl(original_shipping_fees, 0) original_shipping_fees,
  case
  when facts.seller_key in (2622, 2623, 2624, 2625, 2626) then 'PL'
  when shipping_by = 'Homzmart' then 'MDS'
  when shipping_by = 'Seller' then 'FDS'
  when shipping_by = 'OCF' then 'OCF' end shipping_by_module,
  shipping_by,
  facts.status,
  p.commercial_cat,
  p.commercial_sub_cat as commercial_subcat,
  p.commercial_sub_sub_cat as commercial_sub_subcat,
  last_status_dt,
  last_actual_status_dt,
  last_actual_dc_status_dt,
  last_dc_status,
  last_dc_status_not_blank,
  last_status_not_blank,
  payment_method,
  seller_commission,
  case
  when upper(facts.status) in ('DELIVERED', 'COMPLETE') then 1
  else 0 end NIS_Value,
  discount_amount,
  sales_item_rules_applied,
  coupon_rule_key,
  coupon_code,
  coupon_rule_name,
  coupon_type,
  coupon_rule_discount_amount,
  coupon_rule_action,
  last_del_status_dt last_deliver_date,
  last_rej_status_dt last_reject_date,
  last_return_status_dt,
  last_can_status_dt last_cancel_date,
  last_ready_to_ship_dt,
  last_being_returned_dt,
  last_at_dc_dt,
  first_at_dc_dt,
  last_being_fixed_dt,
  last_refunded_dt,
  last_shipped_dt,
  last_inprogress_dt,
  last_being_exchanged_dt,
  last_dc_canceled_dt,
  last_dc_returned_to_seller_dt,
  last_dc_scheduled_dt,
  last_dc_returned_dt,
  last_c_delivered_dt,
  last_dc_received_dt,
  last_dc_intransit_dt,
  last_dc_rejected_dt,
  last_placed_dt,
  """+redshift_schema+""".date_diff_exc_holiday(last_confirm_status_dt, """+cond_to+""") confirmation_to_date,
  """+redshift_schema+""".date_diff_exc_holiday(last_inprogress_dt, """+cond_to+""") inprogress_to_date,
  """+redshift_schema+""".date_diff_exc_holiday(last_ready_to_ship_dt, """+cond_to+""") rts_to_date,
  """+redshift_schema+""".date_diff_exc_holiday(first_at_dc_dt, """+cond_to+""") first_at_dc_to_date,
  """+redshift_schema+""".date_diff_exc_holiday(last_at_dc_dt, """+cond_to+""") last_at_dc_to_date,
  """+redshift_schema+""".date_diff_exc_holiday(item_created_at_dt, """+cond_to+""") creation_to_date,
  """+redshift_schema+""".date_diff_exc_holiday(last_placed_dt, last_confirm_status_dt) places_to_confirmed,
  """+redshift_schema+""".date_diff_exc_holiday(last_confirm_status_dt, last_inprogress_dt) confirmed_to_inprogress,
  """+redshift_schema+""".date_diff_exc_holiday(last_inprogress_dt, last_ready_to_ship_dt) inprogress_to_rts,
  """+redshift_schema+""".date_diff_exc_holiday(last_ready_to_ship_dt, last_del_status_dt) rts_to_delivered,
  """+redshift_schema+""".date_diff_exc_holiday(last_ready_to_ship_dt, first_at_dc_dt) rts_to_atdc,
  """+redshift_schema+""".date_diff_exc_holiday(last_placed_dt, last_del_status_dt) placed_to_delivered,
  """+redshift_schema+""".date_diff_exc_holiday(last_confirm_status_dt, last_del_status_dt) confirmed_to_delivered,
  """+redshift_schema+""".date_diff_exc_holiday(first_at_dc_dt, last_del_status_dt) atdc_to_delivered,
  """+redshift_schema+""".date_diff_exc_holiday(last_inprogress_dt, last_del_status_dt) inprogress_to_delivered,
  """+redshift_schema+""".date_diff_exc_weekend(first_at_dc_dt, last_dc_canceled_dt) atdc_to_dccanceled_wk,
  """+redshift_schema+""".date_diff_exc_weekend(first_at_dc_dt, last_dc_returned_dt) atdc_to_dcreturned_wk,
  """+redshift_schema+""".date_diff_exc_weekend(first_at_dc_dt, last_dc_scheduled_dt) atdc_to_dcscheduled_wk,
  """+redshift_schema+""".date_diff_exc_weekend(first_at_dc_dt, last_del_status_dt) atdc_to_delivered_wk,
  """+redshift_schema+""".date_diff_exc_weekend(first_at_dc_dt, last_rej_status_dt) atdc_to_rejection_wk,
  """+redshift_schema+""".date_diff_exc_weekend(first_at_dc_dt, last_return_status_dt) atdc_to_returned_wk,
  """+redshift_schema+""".date_diff_exc_weekend(last_being_returned_dt, last_dc_returned_dt) beingreturn_to_dcreturned_wk,
  """+redshift_schema+""".date_diff_exc_weekend(last_being_returned_dt, last_rej_status_dt) beingreturn_to_returned_wk,
  """+redshift_schema+""".date_diff_exc_weekend(item_created_at_dt, first_at_dc_dt) creation_to_atdc_wk,
  """+redshift_schema+""".date_diff_exc_weekend(item_created_at_dt, last_can_status_dt) creation_to_canceled_wk,
  """+redshift_schema+""".date_diff_exc_weekend(item_created_at_dt, last_confirm_status_dt) creation_to_confirmed_wk,
  """+redshift_schema+""".date_diff_exc_weekend(item_created_at_dt, last_dc_returned_dt) creation_to_dcreturned_wk,
  """+redshift_schema+""".date_diff_exc_weekend(item_created_at_dt, last_del_status_dt) creation_to_delivered_wk,
  """+redshift_schema+""".date_diff_exc_weekend(item_created_at_dt, last_rej_status_dt) creation_to_rejection_wk,
  """+redshift_schema+""".date_diff_exc_weekend(item_created_at_dt, last_return_status_dt) creation_to_returned_wk,
  """+redshift_schema+""".date_diff_exc_weekend(
    last_dc_canceled_dt,
    last_dc_returned_to_seller_dt
  ) dccancled_to_dcreturntoseller_wk,
  """+redshift_schema+""".date_diff_exc_weekend(
    last_dc_rejected_dt,
    last_dc_returned_to_seller_dt
  ) dcrejected_to_dcreturntoseller_wk,
  """+redshift_schema+""".date_diff_exc_weekend(
    last_dc_returned_dt,
    last_dc_returned_to_seller_dt
  ) dcreturned_to_dcreturntoseller_wk,
  """+redshift_schema+""".date_diff_exc_weekend(last_inprogress_dt, last_ready_to_ship_dt) inprogress_to_readytoship_wk,
  """+redshift_schema+""".date_diff_exc_weekend(last_inprogress_dt, last_del_status_dt) inprogress_to_delivered_wk,
  """+redshift_schema+""".date_diff_exc_weekend(last_ready_to_ship_dt, first_at_dc_dt) readytoship_to_atdc_wk,
  """+redshift_schema+""".date_diff_exc_weekend(last_ready_to_ship_dt, last_del_status_dt) readytoship_to_delivered_wk,
  """+redshift_schema+""".date_diff_exc_weekend(last_rej_status_dt, last_dc_rejected_dt) rejected_to_dcrejected_wk,
  """+redshift_schema+""".date_diff_exc_weekend(last_return_status_dt, last_dc_returned_dt) returned_to_dcreturned_wk,
  """+redshift_schema+""".date_diff_exc_weekend(first_at_dc_dt, """+cond_to+""") atdc_to_today_wk,
  """+redshift_schema+""".date_diff_exc_weekend(
    last_being_returned_dt,
    last_dc_returned_to_seller_dt
  ) beingreturned_to_returntoseller_wk,
  """+redshift_schema+""".date_diff_exc_weekend(last_confirm_status_dt, last_inprogress_dt) confirmed_to_inprogess_wk,
  """+redshift_schema+""".date_diff_exc_weekend(last_confirm_status_dt, """+cond_to+""") confirmed_to_today_wk,
  """+redshift_schema+""".date_diff_exc_weekend(item_created_at_dt, """+cond_to+""") creation_to_today_wk,
  facts.product_key,
  case
  when mobile_order = 1 then 'App'
  else 'Website' end mobile_order,
  ras.store_name,
  facts.owner_name,
  to_timestamp(sysdate, 'YYYY-MM-DD HH24:MI:SS'):: TIMESTAMP WITHOUT TIME zone last_refresh_time,
  deposit,
  """+redshift_schema+""".date_diff_exc_holiday(creation_date, last_confirm_status_dt) creation_to_confirmed,
  """+redshift_schema+""".date_diff_exc_holiday(last_ready_to_ship_dt, last_shipped_dt) rts_to_shipped,
  """+redshift_schema+""".date_diff_exc_holiday(last_shipped_dt, last_del_status_dt) shipped_to_deliver,
  """+redshift_schema+""".date_diff_exc_holiday(creation_date, last_del_status_dt) creation_to_deliver,
  """+redshift_schema+""".date_diff_exc_holiday(creation_date, last_reject_date) creation_to_reject,
  """+redshift_schema+""".date_diff_exc_holiday(last_being_returned_dt, last_return_status_dt) beingreturned_to_returned,
  """+redshift_schema+""".date_diff_exc_holiday(creation_date, last_cancel_date) creation_to_cancel,
  """+redshift_schema+""".date_diff_exc_holiday(last_confirm_status_dt, last_cancel_date) confirmed_to_cancel,
  last_status_reason,
  last_status_sub_reason,
  last_can_status_reason,
  last_can_status_sub_reason,
  last_rej_status_reason,
  last_rej_status_sub_reason,
  last_return_status_reason,
  last_return_status_sub_reason,
  address_region,
  address_city,
  """+redshift_schema+""".date_diff_exc_weekend(last_ready_to_ship_dt, """+cond_to+"""):: float8 rts_to_today_wk,
  p.commercial_material as commercial_material,
  facts.shipping_value,
  facts.item_value,
  facts.is_shipping_inclusive,
  facts.parent_item_key,
  facts.item_created_at_ts as creation_time,
  facts.last_status_time,
  facts.last_status_agent,
  facts.last_del_status_time as last_deliver_time,
  facts.last_del_status_agent as last_deliver_agent,
  facts.last_can_status_time as last_cancel_time,
  facts.last_can_status_agent as last_cancel_agent,
  facts.last_rej_status_time as last_reject_time,
  facts.last_rej_status_agent as last_reject_agent,
  facts.last_return_status_time,
  facts.last_return_status_agent,
  facts.last_confirm_status_time,
  facts.last_confirm_status_agent,
  facts.last_ready_to_ship_time,
  facts.last_ready_to_ship_agent,
  facts.last_being_returned_time,
  facts.last_being_returned_agent,
  facts.first_at_dc_time,
  facts.first_at_dc_agent,
  facts.last_shipped_time,
  facts.last_shipped_agent,
  facts.last_inprogress_time,
  facts.last_inprogress_agent,
  facts.item_created_at_dt creation_date_dt,
  facts.cod_fees,
  facts.first_del_status_dt as first_deliver_date,
  facts.first_del_status_time as first_deliver_time,
  facts.first_del_status_agent as first_deliver_agent
from
  ( select * from """+redshift_schema+""".sales_order_item_accum_facts where item_updated_at_dt >= """+cond_from+""") facts
  left outer join """+redshift_schema+""".retail_app_order_items raoi on sales_item_key = raoi.item_key
  left outer join """+redshift_schema+""".retail_app_employees rae on raoi.client_key = rae.client_key
  left outer join """+redshift_schema+""".retail_app_stores ras on rae.store_key = ras.store_key
  left outer join """+redshift_schema+""".product p on facts.product_key = p.product_key
  left outer join """+redshift_schema+""".comm_sub_old on facts.sales_item_key = comm_sub_old.id
  left outer join """+redshift_schema+""".comm_sub on facts.sku = comm_sub.sku
  and facts.item_created_at_dt between comm_sub.sub_start_date
  and comm_sub.sub_end_date
where
  item_updated_at_dt >= """+cond_from


sqlquey = spark.read.format('jdbc')\
.option ("url",url)\
.option("query",SqlQuery0)\
.option ("user",redshift_username)\
.option("password",redshift_password)\
.load()

sqlqueyDF = DynamicFrame.fromDF(sqlquey, glueContext, "sqlqueyDF")

# Script generated for node AWS Glue Data Catalog

pre_query = """drop table if exists """+redshift_schema+""".eg_sales_cube_stage ;create table """+redshift_schema+""".eg_sales_cube_stage as select * from """+redshift_schema+""".eg_sales_cube where 1=2;"""


# post_query = """begin; 
#                 insert into """+redshift_schema+""".eg_sales_cube_stage select * from  """+redshift_schema+""".eg_sales_cube_stage where item_updated_at_dt between """+cond_from+""" and """+cond_to+""";
#                 end;"""  


eg_sales_cube_redshift = glueContext.write_dynamic_frame.from_jdbc_conf(
  frame = sqlqueyDF,
  catalog_connection = redshift_conn,
  connection_options = {
    "database": redshift_dbname,
    "dbtable": redshift_schema+".eg_sales_cube_stage",
    "preactions": pre_query,
    # "postactions": post_query,
  },
  redshift_tmp_dir = redshift_temp,
)

job.commit()