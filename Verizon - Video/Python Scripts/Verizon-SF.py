#!/usr/bin/env python
import snowflake.connector
import time

#Log In Variables 
ACCOUNT = 'verizon_viva.us-east-1'
USER = 'Cirwin'
PASSWORD = 'Cmimln!43'

#Snowflake Warehouse
WAREHOUSE = 'Load_WH'

#Add Schema and database together for a single transaction
DATABASE = 'Viva_POC'

#Total number of days
days = 365
i = 0


# Create Connections
ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT
    )

#Creates Cursor for session
cs = ctx.cursor()

#Create Query 
for i in range(0,(days+1)):

    dayValue = days - i
    sfQuery = ""

    header = ""
    if (i == 0 ):

        #Which warehouse to use. 
        usingWarehouse = "Use warehouse Load_WH;"
        cs.execute(usingWarehouse)

        #which database to use
        usingDatabase =  "Use Database Viva_POC;"
        cs.execute(usingDatabase)

        usingSchema = "use Schema Stage;"
        cs.execute(usingSchema)

        header = "Create or replace table analytics_video.FACT_TUNING_CHANNEL_RPT as  "
    else:
        header = "insert into analytics_video.FACT_TUNING_CHANNEL_RPT "

    #Execute query 
    sfQuery = ''.join([header  , "select stgf.TRANSACTION_RECORD_ID, "
     , "stgf.INSERT_RECORD_TS, "
     , "stgf.VHO_ID, "
     , "stgf.STB_TYPE, "
     , "stgf.STB, "
     , "stgf.BTCH_NBR, "
     , "stgf.LOGIN_TM, "
     , "stgf.METRIC_TYPE, "
     , "stgf.VZ_TUNER_WRPR, "
     , "stgf.ACTVTY_TS, "    
     , "stgf.ACTVTY_SEQ_NBR,"
     , "stgf.APP_ID,"
     , "stgf.TUNING_INFO,"
     , "stgf.VIRT_CHNL_NBR, " 
     , "stgf.CLIENT_ID, "
     , "stgf.VMS_ID, "
     , "stgf.CHANNEL_INDEX, "
     , "stgf.CHANNEL_NUMBER, "
     , "dd.my_date::date as CHANNEL_DT, "
     , "CONCAT(dd.my_date::string , \' \' , CAST(stgf.CHANNEL_TS as Time)::string)::datetime as CHANNEL_TS, "
     , "stgf.SECONDS::Number as Seconds, "
     ,"stgf.PROGRAM_INFO, " 
     ,"stgf.SVC_ID, "
     ,"stgf.STRSTATIONNAME, "
     ,"stgf.STRSTATIONCALLSIGN, " 
     ,"stgf.OMNI_CATEGORY, "
     ,"stgf.OMNI_CHANNEL_NAME, " 
     ,"stgf.INCLUDE_IN_OMNI_TOP_30, " 
     ,"stgf.INCLUDE_IN_PACKAGE_COMPARISON, " 
     ,"stgf.EXCLUDE_COMPLETELY, "
     ,"stgf.HD_IND,"
     , "stgf.OMNI_CHANNEL_NAME_COMBINED,"
     , "stgf.CHANNEL_ROLLUP,"
     , "stgf.IS_LOCAL,"
     , "stgf.IS_SPANISH,"
     , "stgf.INCLUDE_IN_NBO,"
     , "stgf.MOB_IN_HOME,"
     , "stgf.MOB_OUT_OF_HOME,"
     , "stgf.IS_SPORTS,"
     , "stgf.CUSTOM_GENRE, "
     ,"stgf.VCOS_GENRE, "
     ,"stgf.COMBINED_GENRE, "
     ,"stgf.PROGRAMMER, "
     ,"stgf.PROGRAMMER_BUNDLE, "
     ,"stgf.CHANNEL, "
     ,"stgf.USE_IN_PRGRAMMER_FLAG, "
     ,"stgf.ACTIVATE_DT, "
     ,"stgf.DEACTIVATE_DT, "
     ,"stgf.ACCT_SK, "
     ,"stgf.RUN_DATE, "
     ,"stgf.PKG_ESTBD_DT, "
     ,"stgf.PACKAGE, "
     ,"stgf.ETHNICITY, "
     ,"stgf.LIFE_STAGE, "
     ,"stgf.NEEDS_BASED_SEGMENT," 
     ,"stgf.CUSTOMER_AGE, "
     ,"stgf.CNX_CD  "   
     ,"from stage.stg_flat_tuning_channel stgf join stage.date_dimension dd on datediff(day, dd.my_date , '2009-04-11') between " + str(dayValue) + " and " + str(dayValue)])
    
    #print ('Starting Insert for iteration ' + str(dayValue),sfQuery )

    cs.execute(sfQuery)

    print ("Query Loaded data for " + str(i) + " days" )

cs.close()

print('All data loaded. Closing Connection....')    

ctx.close()

print('Connection closed')


