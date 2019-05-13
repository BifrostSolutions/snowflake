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
Schema = 'TestingAutomation'

start_Year = 1990

end_Year = 2030

###########################################################
###############    Execute Process    #####################

# Create Snowflake Session
#ctx = TeknionSnowflake.Create_Snowflake_Context(Account, User, Password)

#Create Session for Snowflake query to execute.
#cs = ctx.cursor()

##Set Warehouse, SChema and database. 
#TeknionSnowflake.Set_Snowflake_Query_Attributes(cs, Warehouse, Database, Schema)

##Create the Dimensional Model 
TeknionSnowflake.Create_Dimensional_Model(cs, Schema)

##Load Dimiensional Model
print('Loading Date Dimension....')
TeknionSnowflake.Load_Snowflake_Date_Dimension(cs, Schema, start_Year, end_Year)
print('Date Dimension Loaded....')


##Execute History Load
TeknionSnowflake.Execute_History_Load(cs, Schema)


##Close Session
cs.close()

#Closing Connection
ctx.close()

print('Connection closed')


#print('Creating Dimensional Model')
#TeknionSnowflake.Create_Query_History_Dimension_Model(cs, Schema)

#

#TeknionSnowflake.Create_Snowflake_Date_Dimension(cs, Schema, TableName)

#

#TeknionSnowflake.Load_Snowflake_Date_Dimension(cs, Schema, TableName, start_Year, end_Year)

#print('Date Dimension Loaded')