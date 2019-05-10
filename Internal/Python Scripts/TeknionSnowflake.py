###################  Import Libraies    ###################
import snowflake.connector
import time
import datetime
from datetime import date
from datetime import timedelta


###########################################################
###################  Define Functions   ###################
###########################################################


############    CreateSnowflakeContext     ################
#Creates Snowflake connection object
def Create_Snowflake_Context(ACCOUNT, USER, PASSWORD):
    #Log In Variables 
    

    # Create Connections
    ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT
    )

    return ctx

############    CreateSnowflakeDateDim     ################
def Create_Snowflake_Date_Dimension(SESSION, SCHEMA, TABLENAME):
    createTable = ' '.join(["CREATE or REPLACE TABLE" 
        , SCHEMA
        , "." 
        , TABLENAME
	    ," ( Date_Key INT , "
		," Date DATETIME, "
		," FullDateDDMM String, "
		," FullDateMMDD string, "
		," DayOfMonth NUMBER,  "
		," DaySuffix string,  "
		," DayName String,   "
		," DayOfWeek NUMBER, "
		," DayOfWeekInMonth NUMBER, "
		," DayOfWeekInYear NUMBER, "
		," DayOfQuarter NUMBER, "
		," DayOfYear NUMBER, "
		," WeekOfMonth NUMBER,  "
		," WeekOfQuarter number, "
		," WeekOfYear Number, "
		," Month Number,  "
		," MonthName String, "
		," MonthOfQuarter Number, "
		," Quarter Number, "
		," QuarterName String,"
		," Year Number, "
		," YearName String,  "
		," MonthYear String,  "
		," MMYYYY Number, "
		," FirstDayOfMonth DATE, "
		," LastDayOfMonth DATE, "
		," FirstDayOfQuarter DATE, "
		," LastDayOfQuarter DATE, "
		," FirstDayOfYear DATE, "
		," LastDayOfYear DATE, "
		," IsWeekday NUMBER "
        , " );"])

    print('Creating Table ' + SCHEMA + '.' + TABLENAME + '....')

    SESSION.execute(createTable)

    print('Table ' + SCHEMA + '.' + TABLENAME + ' Created....')

############    LoadSnowflakeDateDim     ###################
def Load_Snowflake_Date_Dimension(SESSION, SCHEMA, TABLENAME, START_YEAR, END_YEAR):

    AllDates = get_Date_Values(START_YEAR, END_YEAR)

    for i in range(0,len(AllDates)):

        insertQuery = " Insert into " + SCHEMA + "." + TABLENAME + " Select "

        for date in AllDates[i]:
            
            insertQuery = insertQuery + "'" + str(date) + "', "
    
        dimDateInsert = insertQuery[0:(len(insertQuery) -2)] + ';'

        SESSION.execute(dimDateInsert)

############    SetSnowflakeQueryAttributes     ###########

def Set_Snowflake_Query_Attributes(SESSION, WAREHOUSE, DATABASE, SCHEMA):
    usingWarehouse = "Use warehouse " + WAREHOUSE + ";"
    SESSION.execute(usingWarehouse)

    #which database to use
    usingDatabase =  "Use Database " + DATABASE + ";"
    SESSION.execute(usingDatabase)

    usingSchema = "use Schema " + SCHEMA +  ";"
    SESSION.execute(usingSchema)

def Create_Query_History_Dimension_Model(SESSION, SCHEMA):
    Create_Snowflake_Schema(SESSION, SCHEMA)
    Create_Fact_Query(SESSION, SCHEMA)
    Create_Dim_Database(SESSION, SCHEMA)
    Create_Dim_Execution_Status(SESSION, SCHEMA)
    Create_Dim_Query_Details(SESSION, SCHEMA)
    Create_Dim_Query_Error(SESSION, SCHEMA)
    Create_Dim_Query_Type(SESSION, SCHEMA)
    Create_Dim_Role(SESSION, SCHEMA)
    Create_Dim_Schema(SESSION, SCHEMA)
    Create_Dim_Session(SESSION, SCHEMA)
    Create_Dim_User(SESSION, SCHEMA)
    Create_Dim_Warehouse(SESSION, SCHEMA)
    Create_Dim_Warehouse_Size(SESSION, SCHEMA)
    Create_Dim_Warehouse_Type(SESSION, SCHEMA)
    Create_InsertLog(SESSION, SCHEMA)
    
############    Execute_History_Load     ########################

#Imports Text file with all the queries
#def Execute_History_Load(SESSION, SCHEMA):

    #Load File
    #f = open(file_Location, 'r')
    #content = f.read()
    #Queries = content.split(';')
    #print(len(Queries))
    #print(content)
    #f.close()

    #print('Starting Data Load....')

    #for query in Queries:
        #if(len(query) > 1):
            #queryToRun = query + ';'
            #SESSION.execute(queryToRun)

    #print('All data loaded. Closing Connection....')   

############    lastDayofMonth    #########################
def last_Day_Of_Month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this is to offset for Feb
    return next_month - datetime.timedelta(days=next_month.day)

############    getDateValues     #########################

def get_Date_Values(start_Year, End_Year):
    startYear = start_Year
    endYear = End_Year
    Year = 0
    Month = 0
    Quarter = 0 
    Dates = []

    #DayOfWeekInYear
    SundayOfWeekInYear = 0
    MondayOfWeekInYear = 0
    TuesdayOfWeekInYear = 0
    WednesdayOfWeekInYear = 0
    ThursdayOfWeekInYear = 0
    FridayOfWeekInYear = 0
    SaturdayOfWeekInYear = 0

    #DayOfWeekInMonth
    SundayOfWeekInMonth = 0
    MondayOfWeekInMonth = 0
    TuesdayOfWeekInMonth = 0
    WednesdayOfWeekInMonth = 0
    ThursdayOfWeekInMonth = 0
    FridayOfWeekInMonth = 0
    SaturdayOfWeekInMonth = 0

    #DayOfWeekInQuarter
    SundayOfWeekInQuarter = 0
    MondayOfWeekInQuarter = 0
    TuesdayOfWeekInQuarter = 0
    WednesdayOfWeekInQuarter = 0
    ThursdayOfWeekInQuarter = 0
    FridayOfWeekInQuarter = 0
    SaturdayOfWeekInQuarter = 0

    #Week of Values
    WeekofYear = 0
    WeekofMonth = 0
    WeekofQuarter = 0
    WeekValue = 1
    WeekMonthValue = 1
    WeekQuarterValue = 1

    #Date Changes
    yearChange = 0
    monthChange = 0
    quarterChange = 0

    ################Start Process##############################

    d0 = date(startYear, 1, 1)
    d1 = date(endYear, 1, 1)
    delta = d1 - d0

    for i in range(0,delta.days):
        newDate = d0 + timedelta(days=i)

        if(newDate.year == int(Year)):
            yearChange = 0
        else:
            yearChange = 1

        if(yearChange == 1):
            #Reset Day of Year Variables
            SundayOfWeekInYear = 0
            MondayOfWeekInYear = 0
            TuesdayOfWeekInYear = 0
            WednesdayOfWeekInYear = 0
            ThursdayOfWeekInYear = 0
            FridayOfWeekInYear = 0
            SaturdayOfWeekInYear = 0

            #Reset Total day of Year Count
            DayOfYear = 0

        if(newDate.month == int(Month)):
            monthChange = 0
        else:
            monthChange = 1

        if(monthChange == 1):
            #Reset Day of Month Variables
            SundayOfWeekInMonth = 0
            MondayOfWeekInMonth = 0
            TuesdayOfWeekInMonth = 0
            WednesdayOfWeekInMonth = 0
            ThursdayOfWeekInMonth = 0
            FridayOfWeekInMonth = 0
            SaturdayOfWeekInMonth = 0
            WeekofMonth = 0
            WeekMonthValue = 1

        #Find date Strings for calculated Date
        Date_Key = newDate.strftime("%Y%m%d") #Date_Key
        Date = newDate.strftime("%Y-%m-%d") #Date
        FullDateDDMM = newDate.strftime("%d/%m/%Y") #FullDateDDMM
        FullDateMMDD = newDate.strftime("%m/%d/%Y") #FullDateMMDD
        DayOfMonth = newDate.day #DayOfMonth
        Month = newDate.month #Month
        Year = newDate.strftime("%Y") #Year
        YearName = newDate.strftime("CY %Y") #YearName
        MMYYY = newDate.strftime("%m%Y") #MMYYY
        FirstDayofMonth = newDate.strftime("%m/01/%Y")
        LastDayofMonth = last_Day_Of_Month(newDate).strftime("%m/%d/%Y")
        FirstDayofYear = newDate.strftime("%Y-01-01") 
        LastDayofYear = newDate.strftime("%Y-12-31") 
        DayOfYear = DayOfYear + 1


        #Quarter values
        if(newDate.month == 1 or newDate.month == 2 or newDate.month == 3):
            Quarter = 1
            QuarterName = 'First'
            FirstDayofQuarter = newDate.strftime("%Y-01-01")
            LastDayOfQuarter = newDate.strftime("%Y-03-31")

        elif(newDate.month == 4 or newDate.month == 5 or newDate.month == 6):
            Quarter = 2
            QuarterName = 'Second'
            FirstDayofQuarter = newDate.strftime("%Y-04-01")
            LastDayOfQuarter = newDate.strftime("%Y-06-30")

        elif(newDate.month == 7 or newDate.month == 8 or newDate.month == 9):
            Quarter = 3
            QuarterName = 'Third'
            FirstDayofQuarter = newDate.strftime("%Y-07-01")
            LastDayOfQuarter = newDate.strftime("%Y-09-30")

        elif(newDate.month == 10 or newDate.month == 11 or newDate.month == 12):
            Quarter = 4
            QuarterName = 'Fourth'
            FirstDayofQuarter = newDate.strftime("%Y-10-01")
            LastDayOfQuarter = newDate.strftime("%Y-12-31")


        if(FirstDayofQuarter == FullDateMMDD):
            quarterChange = 1
        else:
            quarterChange = 0

        if(quarterChange == 1):
            #DayOfWeekInQuarter
            SundayOfWeekInQuarter = 0
            MondayOfWeekInQuarter = 0
            TuesdayOfWeekInQuarter = 0
            WednesdayOfWeekInQuarter = 0
            ThursdayOfWeekInQuarter = 0
            FridayOfWeekInQuarter = 0
            SaturdayOfWeekInQuarter = 0
            WeekQuarterValue = 1


        #Day Suffix
        if(DayOfMonth == 1):
            DaySuffix = str(DayOfMonth) + 'st'
        elif(DayOfMonth == 2):
            DaySuffix = str(DayOfMonth) + 'nd'
        elif(DayOfMonth == 3):
            DaySuffix = str(DayOfMonth) + 'rd'
        else:
            DaySuffix = str(DayOfMonth) + 'th'

        #DayOfWeek
        if(newDate.weekday() == 6):
            DayOfWeek = 1 #DayOfWeek

        else:
            DayOfWeek = newDate.weekday() + 2

        #DayName 
        if(DayOfWeek == 1):
            DayName = 'Sunday'
            SundayOfWeekInYear = SundayOfWeekInYear + 1
            DayOfWeekInYear = SundayOfWeekInYear

            SundayOfWeekInMonth = SundayOfWeekInMonth + 1
            DayOfWeekInMonth = SundayOfWeekInMonth

            SundayOfWeekInQuarter = SundayOfWeekInQuarter + 1
            DayofWeekInQuarter = SundayOfWeekInQuarter

            WeekValue = WeekofYear + 1
            WeekMonthValue = WeekofMonth + 1  
            WeekQuarterValue = WeekofQuarter + 1 
            
            
        elif(DayOfWeek == 2):
            DayName = 'Monday'
            MondayOfWeekInYear = MondayOfWeekInYear + 1
            DayOfWeekInYear = MondayOfWeekInYear

            MondayOfWeekInMonth = MondayOfWeekInMonth + 1
            DayOfWeekInMonth = MondayOfWeekInMonth

            MondayOfWeekInQuarter = MondayOfWeekInQuarter + 1
            DayofWeekInQuarter = MondayOfWeekInQuarter

        elif(DayOfWeek == 3):
            DayName = 'Tuesday'
            TuesdayOfWeekInYear = TuesdayOfWeekInYear + 1
            DayOfWeekInYear = TuesdayOfWeekInYear

            TuesdayOfWeekInMonth = TuesdayOfWeekInMonth + 1
            DayOfWeekInMonth = TuesdayOfWeekInMonth

            TuesdayOfWeekInQuarter = TuesdayOfWeekInQuarter + 1
            DayofWeekInQuarter = TuesdayOfWeekInQuarter

        elif(DayOfWeek == 4):
            DayName = 'Wednesday'
            WednesdayOfWeekInYear = WednesdayOfWeekInYear + 1
            DayOfWeekInYear = WednesdayOfWeekInYear

            WednesdayOfWeekInMonth = WednesdayOfWeekInMonth + 1
            DayOfWeekInMonth = WednesdayOfWeekInMonth

            WednesdayOfWeekInQuarter = WednesdayOfWeekInQuarter + 1
            DayofWeekInQuarter = WednesdayOfWeekInQuarter

        elif(DayOfWeek == 5):
            DayName = 'Thrusday'
            ThursdayOfWeekInYear = ThursdayOfWeekInYear + 1
            DayOfWeekInYear = ThursdayOfWeekInYear

            ThursdayOfWeekInMonth = ThursdayOfWeekInMonth + 1
            DayOfWeekInMonth = ThursdayOfWeekInMonth

            ThursdayOfWeekInQuarter = ThursdayOfWeekInQuarter + 1
            DayofWeekInQuarter = ThursdayOfWeekInQuarter

        elif(DayOfWeek == 6):
            DayName = 'Friday'
            FridayOfWeekInYear = FridayOfWeekInYear + 1
            DayOfWeekInYear = FridayOfWeekInYear

            FridayOfWeekInMonth = FridayOfWeekInMonth + 1
            DayOfWeekInMonth = FridayOfWeekInMonth

            FridayOfWeekInQuarter = FridayOfWeekInQuarter + 1
            DayofWeekInQuarter = FridayOfWeekInQuarter

        elif(DayOfWeek == 7):
            DayName = 'Saturday'
            SaturdayOfWeekInYear = SaturdayOfWeekInYear + 1
            DayOfWeekInYear = SaturdayOfWeekInYear

            SaturdayOfWeekInMonth = SaturdayOfWeekInMonth + 1
            DayOfWeekInMonth = SaturdayOfWeekInMonth

            SaturdayOfWeekInQuarter = SaturdayOfWeekInQuarter + 1
            DayofWeekInQuarter = SaturdayOfWeekInQuarter

        #Month
        if(newDate.month == 1):
            MonthName = 'January'
            MonthYear = newDate.strftime("Jan-%Y")
            MonthOfQuarter = 1

        elif(newDate.month == 2):
            MonthName = 'February'
            MonthYear = newDate.strftime("Feb-%Y")
            MonthOfQuarter = 2

        elif(newDate.month == 3):
            MonthName = 'February'
            MonthYear = newDate.strftime("Feb-%Y")
            MonthOfQuarter = 3

        elif(newDate.month == 4):
            MonthName = 'April'
            MonthYear = newDate.strftime("Apr-%Y")
            MonthOfQuarter = 1

        elif(newDate.month == 5):
            MonthName = 'May'
            MonthYear = newDate.strftime("May-%Y")
            MonthOfQuarter = 2

        elif(newDate.month == 6):
            MonthName = 'June'
            MonthYear = newDate.strftime("Jun-%Y")
            MonthOfQuarter = 3

        elif(newDate.month == 7):
            MonthName = 'July'
            MonthYear = newDate.strftime("Jul-%Y")
            MonthOfQuarter = 1

        elif(newDate.month == 8):
            MonthName = 'August'
            MonthYear = newDate.strftime("Aug-%Y")
            MonthOfQuarter = 2

        elif(newDate.month == 9):
            MonthName = 'September'
            MonthYear = newDate.strftime("Sep-%Y")
            MonthOfQuarter = 3

        elif(newDate.month == 10):
            MonthName = 'October'
            MonthYear = newDate.strftime("Oct-%Y")
            MonthOfQuarter = 1

        elif(newDate.month == 11):
            MonthName = 'November'
            MonthYear = newDate.strftime("Nov-%Y")
            MonthOfQuarter = 2

        else:
            MonthName = 'December'
            MonthYear = newDate.strftime("Dec-%Y")
            MonthOfQuarter = 3


        #Week
        WeekofYear = WeekValue 
        WeekofMonth = WeekMonthValue
        WeekofQuarter = WeekQuarterValue 


        #Is the day a workday?
        if(DayOfWeek == 7 or DayOfWeek == 1):
            IsWeekDay = 0
        else:
            IsWeekDay = 1


        
        Dates.append([Date_Key, Date, FullDateDDMM, FullDateMMDD, DayOfMonth, DaySuffix, DayName, DayOfWeek, DayOfWeekInMonth, DayOfWeekInYear, DayofWeekInQuarter, DayOfYear
        , WeekofMonth, WeekofQuarter, WeekofYear, Month, MonthName, MonthOfQuarter, Quarter, QuarterName, Year, YearName, MonthYear, MMYYY, FirstDayofMonth, LastDayofMonth
        , FirstDayofQuarter, LastDayOfQuarter, FirstDayofYear, LastDayofYear, IsWeekDay])

    return Dates

###############    Create Session Dim     #########################
def Create_Dim_Session(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".Dim_Session ( "
    , " Session_Key int identity(1,1) "
    , " , Session_ID String "
    , " , Created_Date datetime default CURRENT_TIMESTAMP() "
    , " , Modified_Date datetime default CURRENT_TIMESTAMP() "
    , " , IsActive Number default 1 "
    , " ); "])

    SESSION.execute(queryText)

    queryTextInsert = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_Session "
    , " Select -1 "
    , " , NULL "
    , " , CURRENT_TIMESTAMP()::dateTime "
    , " , CURRENT_TIMESTAMP()::dateTime "
    , " ,1;"])

    SESSION.execute(queryTextInsert)


###############    Create Query Details Dim     #########################
def Create_Dim_Query_Details(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".Dim_Query_Details "
    , "( "
    , "   Query_Detail_Key int identity(1,1) "
    , " , Query_Text String "
    , " , Created_Date datetime default CURRENT_TIMESTAMP() "
    , " , Modified_Date datetime default CURRENT_TIMESTAMP() "
    , " , IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_Query_Details "
    , "Select -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1; "])

    SESSION.execute(queryText)

###############    Create Database Dim     #########################
def Create_Dim_Database(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".dim_Database "
    , "( "
    , "Database_Key int identity(1,1) "
    , ", Database_Name String "
    , ", Created_Date datetime default CURRENT_TIMESTAMP() "
    , ", Modified_Date datetime default CURRENT_TIMESTAMP() "
    , ", IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_Database "
    , "Select -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1;  "])

    SESSION.execute(queryText)

###############    Create Schema Dim     #########################
def Create_Dim_Schema(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".dim_Schema "
    , "( "
    , "Schema_Key int identity(1,1) "
    , ", Schema_Name String "
    , ", Created_Date datetime default CURRENT_TIMESTAMP() "
    , ", Modified_Date datetime default CURRENT_TIMESTAMP() "
    , ", IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_Schema "
    , "Select -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1;  "])

    SESSION.execute(queryText)

###############    Create Query_Type Dim     #########################
def Create_Dim_Query_Type(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".dim_Query_Type "
    , "( "
    , "Query_Type_Key int identity(1,1) "
    , ", Query_Type_Name String "
    , ", Created_Date datetime default CURRENT_TIMESTAMP() "
    , ", Modified_Date datetime default CURRENT_TIMESTAMP() "
    , ", IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_Query_Type "
    , "Select -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1;  "])

    SESSION.execute(queryText)

###############    Create User Dim     #########################
def Create_Dim_User(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".dim_User "
    , "( "
    , "User_Key int identity(1,1) "
    , ", User_Name String "
    , ", Created_Date datetime default CURRENT_TIMESTAMP() "
    , ", Modified_Date datetime default CURRENT_TIMESTAMP() "
    , ", IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_User "
    , "Select -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1;  "])

    SESSION.execute(queryText)


###############    Create Role Dim     #########################
def Create_Dim_Role(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".dim_Role "
    , "( "
    , "Role_Key int identity(1,1) "
    , ", Role_Name String "
    , ", Created_Date datetime default CURRENT_TIMESTAMP() "
    , ", Modified_Date datetime default CURRENT_TIMESTAMP() "
    , ", IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_Role "
    , "Select -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1;  "])

    SESSION.execute(queryText)

###############    Create Warehouse Dim     #########################
def Create_Dim_Warehouse(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".dim_Warehouse "
    , "( "
    , "Warehouse_Key int identity(1,1) "
    , ", Warehouse_Name String "
    , ", Created_Date datetime default CURRENT_TIMESTAMP() "
    , ", Modified_Date datetime default CURRENT_TIMESTAMP() "
    , ", IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_Warehouse "
    , "Select -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1;  "])

    SESSION.execute(queryText)

###############    Create Warehouse_Size Dim     #########################
def Create_Dim_Warehouse_Size(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".dim_Warehouse_Size "
    , "( "
    , " Warehouse_Size_Key int identity(1,1) "
    , ", Warehouse_Size String "
    , ", Created_Date datetime default CURRENT_TIMESTAMP() "
    , ", Modified_Date datetime default CURRENT_TIMESTAMP() "
    , ", IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_Warehouse_Size "
    , "Select -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1;  "])

    SESSION.execute(queryText)

###############    Create  Warehouse_Type Dim     #########################
def Create_Dim_Warehouse_Type(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".dim_Warehouse_Type "
    , "( "
    , " Warehouse_Type_Key int identity(1,1) "
    , ", Warehouse_Type String "
    , ", Created_Date datetime default CURRENT_TIMESTAMP() "
    , ", Modified_Date datetime default CURRENT_TIMESTAMP() "
    , ", IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_Warehouse_Type "
    , "Select -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1;  "])

    SESSION.execute(queryText)
    
###############    Create Execution_Status Dim     #########################
def Create_Dim_Execution_Status(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".dim_Execution_Status "
    , "( "
    , " Execution_Status_Key int identity(1,1) "
    , ", Execution_Status String "
    , ", Created_Date datetime default CURRENT_TIMESTAMP() "
    , ", Modified_Date datetime default CURRENT_TIMESTAMP() "
    , ", IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".Dim_Execution_Status "
    , "Select -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1;  "])

    SESSION.execute(queryText)

###############    Create Query_Error Dim     #########################
def Create_Dim_Query_Error(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".dim_Query_Error "
    , "( "
    , " Error_Key int identity(1,1) "
    , ", Query_ID String "
    , ", Error_Code Number "
    , ", Error_Message String "
    , ", Created_Date datetime default CURRENT_TIMESTAMP() "
    , ", Modified_Date datetime default CURRENT_TIMESTAMP() "
    , ", IsActive Number default 1 "
    , "); "])

    SESSION.execute(queryText)

    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".dim_Query_Error "
    , "Select -1 "
    , ", NULL "
    , ", -1 "
    , ", NULL "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ", CURRENT_TIMESTAMP()::dateTime "
    , ",1;  "])

    SESSION.execute(queryText)

###############    Create InsertLog    #########################
def Create_InsertLog(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".InsertLog "
    , "(RunId int identity(1,1) "
    , ", Total_Records_Imported int "
    , ", Last_QueryFact_ID int "
    , ", FactEndDateTime timestamp_ltz "
    , ", Process_Start timestamp_ltz  "
    , ", Process_End timestamp_ltz "
    , "); "])

    SESSION.execute(queryText)

###############    Create Schema   #########################
def Create_Snowflake_Schema(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Schema "
    , SCHEMA
    , "; "])

    SESSION.execute(queryText)

    

###############    Create Fact_Query Dim     #########################
def Create_Fact_Query(SESSION , SCHEMA):
    queryText = ''.join(["Create or Replace Table "
    , SCHEMA
    , ".Fact_Query "
    , "( "
    , "QueryFact_ID int Identity(1,1) "
    , ", Query_ID String "
    , ", Cluster_Number Number "
    , ", Query_Tag String "
    , ", Start_Time datetime "
    , ", End_Time datetime "
    , ", TOTAL_ELAPSED_TIME NUMBER "
    , ", BYTES_SCANNED NUMBER "
    , ", ROWS_PRODUCED NUMBER "
    , ", COMPILATION_TIME NUMBER "
    , ", EXECUTION_TIME NUMBER "
    , ", QUEUED_PROVISIONING_TIME NUMBER "
    , ", QUEUED_REPAIR_TIME	NUMBER "
    , ", QUEUED_OVERLOAD_TIME NUMBER "
    , ", TRANSACTION_BLOCKED_TIME NUMBER "
    , ", OUTBOUND_DATA_TRANSFER_CLOUD TEXT "
    , ", OUTBOUND_DATA_TRANSFER_REGION TEXT "
    , ", OUTBOUND_DATA_TRANSFER_BYTES NUMBER "
    , ", INBOUND_DATA_TRANSFER_CLOUD TEXT "
    , ", INBOUND_DATA_TRANSFER_REGION TEXT "
    , ", INBOUND_DATA_TRANSFER_BYTES NUMBER "
    , ", Query_Detail_Key Number "
    , ", Session_Key Number "
    , ", Database_Key Number "
    , ", Schema_Key Number "
    , ", Query_Type_Key Number "
    , ", User_Key Number "
    , ", Role_Key Number "
    , ", Warehouse_Key Number "
    , ", Warehouse_Size_Key Number "
    , ", Warehouse_Type_Key Number "
    , ", Execution_Status_Key Number "
    , ", Error_Key Number "
    , ", Start_Date_Key Number "
    , ", End_Date_Key Number "
    , ");  "])

    SESSION.execute(queryText)