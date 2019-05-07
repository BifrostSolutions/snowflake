#!/usr/bin/env python
import snowflake.connector
import time
import pandas

#Log In Variables 
ACCOUNT = 'teknion'
USER = 'Cirwin'
PASSWORD = 'Cmimln!43'

#Snowflake Warehouse
WAREHOUSE = 'Demo_WH'

#Add Schema and database together for a single transaction
DATABASE = 'CIrwin_SandBox'


print('Opening Snowflake Connection....')   
# Create Connections
ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT
    )

#Creates Cursor for session
cs = ctx.cursor()



#####################
####Do Work here!####
#####################




#Close Cursor
cs.close()

print('All data loaded. Closing Connection....')    

#Closing Connection
ctx.close()

print('Connection closed')
