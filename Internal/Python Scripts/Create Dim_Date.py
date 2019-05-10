###################    Import Libraies    #################
import TeknionSnowflake

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
Schema = 'TestingPython'

#Table name
TableName = 'Dim_Date'

#file_Location = "C:\\Users\\cirwin\\OneDrive - Teknion Data Solutions\\Projects\\snowflake\\Internal\\QueryFile.txt"

start_Year = 1975

end_Year = 1976

###########################################################
###############    Execute Process    #####################

# Create Snowflake Session
ctx = TeknionSnowflake.Create_Snowflake_Context(Account, User, Password)

#Create Session for Snowflake query to execute.
cs = ctx.cursor()

##Set Warehouse, SChema and database. 
TeknionSnowflake.Set_Snowflake_Query_Attributes(cs, Warehouse, Database, Schema)

print('Creating Dimensional Model')
TeknionSnowflake.Create_Query_History_Dimension_Model(cs, Schema)

print('Building Date Dimension')

TeknionSnowflake.Create_Snowflake_Date_Dimension(cs, Schema, TableName)

print('Loading Date Dimension....')

TeknionSnowflake.Load_Snowflake_Date_Dimension(cs, Schema, TableName, start_Year, end_Year)

print('Date Dimension Loaded')
cs.close()

#Closing Connection
ctx.close()

print('Connection closed')