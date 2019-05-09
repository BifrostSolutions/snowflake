###################  Import Libraies    ###################
import snowflake.connector
import time

###########################################################
##################     Define Functions     ###############
###########################################################

#Creates Snowflake connection object
def CreateSnowflakeContext(ACCOUNT, USER, PASSWORD):
    #Log In Variables 
    

    # Create Connections
    ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT
    )

    return ctx

#Imports Text file with all the queries
def RunLoadProcess(snowflakeCtx, file_Location, WAREHOUSE, DATABASE, SCHEMA ):

    #Load File
    f = open(file_Location, 'r')
    content = f.read()
    Queries = content.split(';')
    print(len(Queries))
    #print(content)
    f.close()

    #Snowflake Warehouse
    Warehouse = WAREHOUSE

    #Add Schema and database together for a single transaction
    Database = DATABASE

    #Add Schema together for a single transaction
    Schema = SCHEMA


    print('Opening Snowflake Connection....')   
    
    #Creates Cursor for session
    cs = snowflakeCtx.cursor()

    #Which warehouse to use. 
    usingWarehouse = "Use warehouse " + Warehouse + ";"
    cs.execute(usingWarehouse)

    #which database to use
    usingDatabase =  "Use Database " + Database + ";"
    cs.execute(usingDatabase)

    usingSchema = "use Schema " + Schema +  ";"
    cs.execute(usingSchema)

    print('Starting Data Load....')

    for query in Queries:
        if(len(query) > 1):
            queryToRun = query + ';'
            cs.execute(queryToRun)

    #Close Cursor
    cs.close()

    print('All data loaded. Closing Connection....')    

    #Closing Connection
    snowflakeCtx.close()

    print('Connection closed')

###########################################################
##################   Start Process    #####################
###########################################################


###################    Variables    #######################

#Snowflake Account 
Account = 'Teknion'

#Snowflake User Account
User = 'Cirwin'

#User Password
Password = 'Cmimln!43'

#Snowflake Warehouse
Warehouse = 'DEMO_WH'

#Add Schema and database together for a single transaction
Database = 'CIRWIN_Sandbox'

#Add Schema together for a single transaction
Schema = 'QueryHistory'

file_Location = "C:\\Users\\cirwin\\OneDrive - Teknion Data Solutions\\Projects\\snowflake\\Internal\\QueryFile.txt"

SNOWFLAKECTX = CreateSnowflakeContext(Account, User, Password)

RunLoadProcess(SNOWFLAKECTX, file_Location, Warehouse, Database, Schema)

