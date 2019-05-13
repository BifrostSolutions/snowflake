#!/usr/bin/env python
import snowflake.connector
import time

#Log In Variables 
ACCOUNT = 'verizon_viva.us-east-1'
USER = 'Cirwin'
PASSWORD = 'Cmimln!43'

#Snowflake Warehouse
WAREHOUSE = 'DEMO_WH'

#Add Schema and database together for a single transaction
DATABASE = 'Viva_POC'

#Add Schema together for a single transaction
Schema = 'Stage'

#Table that you want to insert
TABLENAME = 'analytics_video.fact_dm_tuning_channel'

#Total number of days
days = 365
i = 0
rangeMax = 74


# Create Connections    
ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT
    )

#Creates Cursor for session
cs = ctx.cursor()

#Create Query 
for i in range(0,rangeMax):

    dayValue = rangeMax - (i)
    sfQuery = ""

    header = ""
    if (i == 0 ):

        dayStartValue = days - (i)
        dayEndValue = days - 4

        #Which warehouse to use. 
        usingWarehouse = "Use warehouse " + WAREHOUSE + ";"
        cs.execute(usingWarehouse)

        #which database to use
        usingDatabase =  "Use Database " + DATABASE + ";"
        cs.execute(usingDatabase)

        usingSchema = "use Schema " + Schema +  ";"
        cs.execute(usingSchema)

        header = "Create or replace table " + TABLENAME + " as  "
    else:
        
        dayStartValue = dayEndValue - 1
        
        dayEndValue = dayEndValue - 5

        header = "insert into " + TABLENAME 

    #Execute query 
    sfQuery = ''.join([header  , "select stgf.TRANSACTION_RECORD_ID, "
     , "stgf.VHO_ID, "
     , "stgf.STB_TYPE, "
     , "stgf.STB, "
     , "stgf.BTCH_NBR, "
     , "stgf.LOGIN_TM::timestamp as LOGIN_TM, "
     , "stgf.METRIC_TYPE, "
     , "stgf.VZ_TUNER_WRPR, "
     , "stgf.ACTVTY_TS::timestamp as ACTVTY_TS, "    
     , "stgf.ACTVTY_SEQ_NBR, "
     , "stgf.APP_ID, "
     , "stgf.CREATN_TS as CREATN_TS, "
     , "stgf.VIRT_CHNL_NBR, "
     , "stgf.CLIENT_ID, "
     , "stgf.VMS_ID, "
     , "stgf.CHANNEL_INDEX, "
     , "stgf.Channel_NUMBER, "
     , "dd.my_date as CHANNEL_DT, "
     , "(dd.my_date::string || \' \' || (stgf.CHANNEL_TS::time)::string)::timestamp as CHANNEL_TS, "
     , "stgf.SECONDS, "
     , "stgf.dim_omni_channel_key, "
     , "stgf.dim_equipment_key, "
     , "stgf.dim_account_key, "
     , "dd.dim_date_key "
     , " from stage.stg_fact_DM_tuning_channel stgf join stage.date_dimension dd on datediff(day, dd.my_date , '2019-04-15') between " + str(dayEndValue) + " and " + str(dayStartValue)]) + " order by CHANNEL_DT, dim_omni_channel_key, dim_account_key;"
    
    print ('Starting Insert for iteration ' + str(dayEndValue), ' to ' , str(dayStartValue) )

    cs.execute(sfQuery)

    print ("Query Loaded data for " + str(dayEndValue), " to " , str(dayStartValue) + " days " )

cs.close()

print('All data loaded. Closing Connection....')    

ctx.close()

#print('Connection closed')


