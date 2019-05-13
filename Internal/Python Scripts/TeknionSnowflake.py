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

    # Create Connections
    ctx = snowflake.connector.connect(
    user=USER,
    password=PASSWORD,
    account=ACCOUNT
    )

    return ctx

############    SetSnowflakeQueryAttributes     ###########

def Set_Snowflake_Query_Attributes(SESSION, WAREHOUSE, DATABASE, SCHEMA):
    usingWarehouse = "Use warehouse " + WAREHOUSE + ";"
    SESSION.execute(usingWarehouse)

    #which database to use
    usingDatabase =  "Use Database " + DATABASE + ";"
    SESSION.execute(usingDatabase)

    #usingSchema = "use Schema " + SCHEMA +  ";"
    #SESSION.execute(usingSchema)

############    Create_Query_History_Dimension_Model     ###########
def Create_Dimensional_Model(SESSION, SCHEMA):

    print('Creating Dimensional Model in Schema ' + SCHEMA)

    Create_Snowflake_Schema(SESSION, SCHEMA)
    Create_Snowflake_Date_Dimension(SESSION, SCHEMA)
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

    print('Dimensional Model Created in Schema ' + SCHEMA)
    
############    Execute_History_Load     ########################

#Imports Text file with all the queries
def Execute_History_Load(SESSION, SCHEMA):
    
    print('Loading Dimensional Model in Schema ' + SCHEMA)

    SESSION.execute("Set Process_Start = (Select Current_Timestamp());")
    SESSION.execute("Set range_Start = (Select Max(FactEndDateTime) from " + SCHEMA + ".InsertLog);")
    SESSION.execute("Set Start_Date = (IFNULL($range_Start, DateAdd(day, -8, Current_TimeStamp())));")
    SESSION.execute("set range_End = Current_Timestamp();")

    Create_Query_History_Stage_Table(SESSION)
    print('Query History Table Created')

    Merge_Dim_Session(SESSION, SCHEMA)
    print('Dim_Session Updated')

    Merge_Dim_Query_Details(SESSION, SCHEMA)
    print('Dim_Query_Details Updated')

    Merge_Dim_Database(SESSION, SCHEMA)
    print('Dim_Database Updated')
    
    Merge_Dim_Schema(SESSION, SCHEMA)
    print('Dim_Schema Updated')

    Merge_Dim_Query_Type(SESSION, SCHEMA)
    print('Dim_Query_Type Updated')

    Merge_Dim_User(SESSION, SCHEMA)
    print('Dim_User Updated')

    Merge_Dim_Role(SESSION, SCHEMA)
    print('Dim_Role Updated')

    Merge_Dim_Warehouse(SESSION, SCHEMA)
    print('Dim_Warehouse Updated')

    Merge_Dim_Warehouse_Size(SESSION, SCHEMA)
    print('Dim_Warehouse_Size Updated')

    Merge_Dim_Warehouse_Type(SESSION, SCHEMA)
    print('Dim_Warehouse_Type Updated')

    Merge_Dim_Execution_Status(SESSION, SCHEMA)
    print('Dim_Execution_Status Updated')

    Merge_Dim_Query_Error(SESSION, SCHEMA)
    print('Dim_Query_Error Updated')

    Merge_Fact_Query(SESSION, SCHEMA)
    print('Fact_Query Updated')

    #Get Total number of records from history table
    SESSION.execute("Set ImportCount = (Select Count(1) from Query_History);")

    Update_InsertLog(SESSION, SCHEMA)
    print('Updating InsertLog Table')

    SESSION.execute("DROP TABLE Query_History;")

    print('Load Completed...')


############    LoadSnowflakeDateDim     ###################
def Load_Snowflake_Date_Dimension(SESSION, SCHEMA, START_YEAR, END_YEAR):

    print('Begin Load of Dim_Date for Years between ' + str(START_YEAR) + ' and ' + str(END_YEAR))

    AllDates = get_Date_Values(START_YEAR, END_YEAR)

    selectQuery = ''

    insertQuery = " Insert into " + SCHEMA + ".Dim_Date Select * from ("

    for i in range(0,len(AllDates)):

        if i == 0:

            selectQuery = selectQuery + ' (Select '
        
        else:
            selectQuery = selectQuery + ') Union ALL (Select '

        columnName = 0
        for date in AllDates[i]:

            selectQuery = selectQuery + "'" + str(date) + "' as Column" + str(columnName) + ", "

            columnName = columnName + 1

        dimDateInsert = (selectQuery[0:(len(selectQuery) -2)] + ')').replace(', )', ')')

    insertQuery =  insertQuery + dimDateInsert + ')'

    print('Inserting data into Dim_Date Table...')
    SESSION.execute(insertQuery)

############    last_Day_Of_Month    #########################
def last_Day_Of_Month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this is to offset for Feb
    return next_month - datetime.timedelta(days=next_month.day)

############    get_Date_Values     #########################

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

############    CreateSnowflakeDateDim     ################
def Create_Snowflake_Date_Dimension(SESSION, SCHEMA):
    createTable = ' '.join(["CREATE or REPLACE TABLE " 
        , SCHEMA
        , ".Dim_Date"
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

    print('Creating Table ' + SCHEMA + '.Dim_Date....')

    SESSION.execute(createTable)

    print('Table ' + SCHEMA + '.Dim_Date Created....')

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
    , ", Query_Type String "
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

###############    Update Insert Log   #########################
def Update_InsertLog(SESSION , SCHEMA):
    #Update Log Table
    queryText = ''.join(["Insert into "
    , SCHEMA
    , ".InsertLog(FactEndDateTime, Last_QueryFact_ID, Total_Records_Imported, Process_Start, Process_End) "
    , " Select Max(End_Time) "
    , ", max(QueryFact_ID) "
    , ", $ImportCount "
    , ", $Process_Start "
    , ", Current_TimeStamp() "
    , "from "
    , SCHEMA
    , ".Fact_query;"])

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
    
###############    Merge Dim Session    #########################
def Merge_Dim_Session(SESSION , SCHEMA):
    queryText = ''.join(["Merge into  "
    , SCHEMA
    , ".Dim_Session as Target "
    ," using(select Distinct Session_ID "
    ,"        from Query_History "
    ,"        where Session_ID is not null) as Source "
    ,"    ON Target.Session_ID = Source.Session_ID "
    ," WHEN NOT Matched "
    ," THEN INSERT (Session_ID "
    ,"            , Created_Date "
    ,"            , Modified_Date) "
    ,"    Values(Source.Session_ID "
    ,"        , CURRENT_TIMESTAMP()::dateTime "
    ,"        , CURRENT_TIMESTAMP()::dateTime); "])

    SESSION.execute(queryText)

###############    Merge Dim Query Details    #########################
def Merge_Dim_Query_Details(SESSION , SCHEMA):
    queryText = ''.join(["Merge into  "
    , SCHEMA
    , ".Dim_Query_Details as Target "
    ," using(select Distinct Query_Text "
    ,"        from Query_History "
    ,"        where Query_Text is not null) as Source "
    ,"    ON Target.Query_Text = Source.Query_Text "
    ," WHEN NOT Matched "
    ," THEN INSERT (Query_Text "
    ,"            , Created_Date "
    ,"            , Modified_Date) "
    ,"    Values(Source.Query_Text "
    ,"        , CURRENT_TIMESTAMP()::dateTime "
    ,"        , CURRENT_TIMESTAMP()::dateTime); "])

    SESSION.execute(queryText)

###############    Merge Dim Database    #########################
def Merge_Dim_Database(SESSION , SCHEMA):
    queryText = ''.join(["Merge into "
    , SCHEMA
    , ".Dim_Database as Target "
    ,"using(select Distinct Database_Name "
    ,"        from Query_History "
    ,"        where database_Name is not null) as Source "
    ,"    ON Target.Database_Name = Source.Database_Name "
    ," WHEN NOT Matched "
    ," THEN INSERT (Database_Name "
    ,"            , Created_Date "
    ,"            , Modified_Date) "
    ,"    Values(Source.Database_Name "
    ,"        , CURRENT_TIMESTAMP()::dateTime "
    ,"        , CURRENT_TIMESTAMP()::dateTime); "])

    SESSION.execute(queryText)

###############    Merge Dim Schema    #########################
def Merge_Dim_Schema(SESSION , SCHEMA):
    queryText = ''.join(["Merge into "
    , SCHEMA
    , ".Dim_Schema as Target "
    ," using(select Distinct Schema_Name "
    ,"        from Query_History "
    ,"        where Schema_Name is not null) as Source "
    ,"    ON Target.Schema_Name = Source.Schema_Name "
    ," WHEN NOT Matched "
    ," THEN INSERT (Schema_Name "
    ,"            , Created_Date "
    ,"            , Modified_Date) "
    ,"    Values(Source.Schema_Name "
    ,"        , CURRENT_TIMESTAMP()::dateTime "
    ,"        , CURRENT_TIMESTAMP()::dateTime); "])

    SESSION.execute(queryText)

###############    Merge Dim User    #########################
def Merge_Dim_User(SESSION , SCHEMA):
    queryText = ''.join([" Merge into "
    , SCHEMA
    , ".dim_User as Target "
    ," using(select Distinct User_Name "
    ,"         from Query_History "
    ,"         where User_Name is not null) as Source "
    ,"     ON Target.User_Name = Source.User_Name "
    ," WHEN NOT Matched "
    ," THEN INSERT (User_Name "
    ,"             , Created_Date "
    ,"             , Modified_Date) "
    ,"     Values(Source.User_Name "
    ,"         , CURRENT_TIMESTAMP()::dateTime "
    ,"         , CURRENT_TIMESTAMP()::dateTime);  "])

    SESSION.execute(queryText)

###############    Merge Dim Query Type    #########################
def Merge_Dim_Query_Type(SESSION , SCHEMA):
    queryText = ''.join(["Merge into "
    , SCHEMA
    , ".dim_Query_Type as Target "
    ," using(select Distinct Query_Type "
    ,"        from Query_History "
    ,"        where Query_Type is not null) as Source "
    ,"    ON Target.Query_Type = Source.Query_Type "
    ," WHEN NOT Matched "
    ," THEN INSERT (Query_Type "
    ,"            , Created_Date "
    ,"            , Modified_Date) "
    ,"    Values(Source.Query_Type "
    ,"        , CURRENT_TIMESTAMP()::dateTime "
    ,"        , CURRENT_TIMESTAMP()::dateTime); "])

    SESSION.execute(queryText)
    
###############    Merge Dim Role   #########################
def Merge_Dim_Role(SESSION , SCHEMA):
    queryText = ''.join(["Merge into "
    , SCHEMA
    , ".dim_Role as Target  "
    ," using(select Distinct Role_Name "
    ,"        from Query_History "
    ,"        where Role_Name is not null) as Source "
    ,"    ON Target.Role_Name = Source.Role_Name "
    ," WHEN NOT Matched "
    ," THEN INSERT (Role_Name "
    ,"            , Created_Date "
    ,"            , Modified_Date) "
    ,"    Values(Source.Role_Name "
    ,"        , CURRENT_TIMESTAMP()::dateTime "
    ,"        , CURRENT_TIMESTAMP()::dateTime); "])

    SESSION.execute(queryText)

###############    Merge Dim Warehouse   #########################
def Merge_Dim_Warehouse(SESSION , SCHEMA):
    queryText = ''.join(["Merge into "
    , SCHEMA
    , ".dim_Warehouse as Target  "
    ," using(select Distinct Warehouse_Name "
    ,"         from Query_History "
    ,"         where Warehouse_Name is not null) as Source "
    ,"     ON Target.Warehouse_Name = Source.Warehouse_Name "
    ," WHEN NOT Matched "
    ," THEN INSERT (Warehouse_Name "
    ,"             , Created_Date "
    ,"             , Modified_Date) "
    ,"     Values(Source.Warehouse_Name "
    ,"         , CURRENT_TIMESTAMP()::dateTime "
    ,"         , CURRENT_TIMESTAMP()::dateTime);  "])

    SESSION.execute(queryText)

###############    Merge Dim Warehouse Size   #########################
def Merge_Dim_Warehouse_Size(SESSION , SCHEMA):
    queryText = ''.join(["Merge into "
    , SCHEMA
    , ".dim_Warehouse_Size as Target  "
    ," using(select Distinct Warehouse_Size "
    ,"         from Query_History "
    ,"         where Warehouse_Size is not null) as Source "
    ,"     ON Target.Warehouse_Size = Source.Warehouse_Size "
    ," WHEN NOT Matched "
    ," THEN INSERT (Warehouse_Size "
    ,"             , Created_Date "
    ,"             , Modified_Date) "
    ,"     Values(Source.Warehouse_Size "
    ,"         , CURRENT_TIMESTAMP()::dateTime "
    ,"         , CURRENT_TIMESTAMP()::dateTime);  "])

    SESSION.execute(queryText)

###############    Merge Dim Warehouse Type   #########################
def Merge_Dim_Warehouse_Type(SESSION , SCHEMA):
    queryText = ''.join(["Merge into "
    , SCHEMA
    , ".dim_Warehouse_Type as Target  "
    ," using(select Distinct Warehouse_Type "
    ,"         from Query_History "
    ,"         where Warehouse_Type is not null) as Source "
    ,"     ON Target.Warehouse_Type = Source.Warehouse_Type "
    ," WHEN NOT Matched "
    ," THEN INSERT (Warehouse_Type "
    ,"             , Created_Date "
    ,"             , Modified_Date) "
    ,"     Values(Source.Warehouse_Type "
    ,"         , CURRENT_TIMESTAMP()::dateTime "
    ,"         , CURRENT_TIMESTAMP()::dateTime);  "])

    SESSION.execute(queryText)

###############    Merge Dim Execution Status  #########################
def Merge_Dim_Execution_Status(SESSION , SCHEMA):
    queryText = ''.join(["Merge into "
    , SCHEMA
    , ".dim_Execution_Status as Target "
    ," using(select Distinct Execution_Status "
    ,"         from Query_History "
    ,"         where Execution_Status is not null) as Source "
    ,"     ON Target.Execution_Status = Source.Execution_Status "
    ," WHEN NOT Matched "
    ," THEN INSERT (Execution_Status "
    ,"             , Created_Date "
    ,"             , Modified_Date) "
    ,"     Values(Source.Execution_Status "
    ,"         , CURRENT_TIMESTAMP()::dateTime "
    ,"         , CURRENT_TIMESTAMP()::dateTime);  "])

    SESSION.execute(queryText)
    
###############    Merge Dim Query Error  ###############################
def Merge_Dim_Query_Error(SESSION , SCHEMA):
    queryText = ''.join(["Merge into "
    , SCHEMA
    , ".dim_Query_Error as Target "
    ," using(select Distinct  "
    ,"         Query_ID "
    ,"         , Error_Code "
    ,"         , Error_Message "
    ,"         from Query_History "
    ,"         where Error_Code is not null) as Source "
    ,"     ON Target.Query_Id = Source.Query_Id "
    ,"     AND  Target.Error_Code = Source.Error_Code "
    ," WHEN NOT Matched "
    ," THEN INSERT (Query_Id "
    ,"             , Error_Code "
    ,"             , Error_Message "
    ,"             , Created_Date "
    ,"             , Modified_Date) "
    ,"     Values(Source.Query_Id "
    ,"         , Source.Error_Code "
    ,"         , Source.Error_Message "
    ,"         , CURRENT_TIMESTAMP()::dateTime "
    ,"         , CURRENT_TIMESTAMP()::dateTime);  "])

    SESSION.execute(queryText)

###############    Merge Dim Execution Status  #########################
def Merge_Fact_Query(SESSION , SCHEMA):
    queryText = ''.join(["Merge Into "
    , SCHEMA
    , ".Fact_Query as Target "
    ," Using(Select qh.Query_ID "
    ,"       , qh.Cluster_Number "
    ,"       , qh.Query_Tag "
    ,"       , qh.Start_Time "
    ,"       , qh.End_Time "
    ,"       , qh.TOTAL_ELAPSED_TIME "
    ,"       , qh.BYTES_SCANNED  "
    ,"       , qh.ROWS_PRODUCED  "
    ,"       , qh.COMPILATION_TIME " 
    ,"       , qh.EXECUTION_TIME  "
    ,"       , qh.QUEUED_PROVISIONING_TIME  "
    ,"       , qh.QUEUED_REPAIR_TIME	 "
    ,"       , qh.QUEUED_OVERLOAD_TIME  "
    ,"       , qh.TRANSACTION_BLOCKED_TIME " 
    ,"       , qh.OUTBOUND_DATA_TRANSFER_CLOUD " 
    ,"       , qh.OUTBOUND_DATA_TRANSFER_REGION  "
    ,"       , qh.OUTBOUND_DATA_TRANSFER_BYTES  "
    ,"       , qh.INBOUND_DATA_TRANSFER_CLOUD  "
    ,"       , qh.INBOUND_DATA_TRANSFER_REGION  "
    ,"       , qh.INBOUND_DATA_TRANSFER_BYTES  "
    ,"       , IFNULL(qd.Query_Detail_Key,-1) as Query_Detail_Key "
    ,"       , IFNULL(dss.Session_Key,-1) as Session_Key "
    ,"       , IFNULL(ddb.Database_Key,-1) as Database_Key "
    ,"       , IFNULL(ds.Schema_Key,-1) as Schema_Key "
    ,"       , IFNULL(dqt.Query_Type_Key,-1) as Query_Type_Key "
    ,"       , IFNULL(du.User_Key,-1) as User_Key "
    ,"       , IFNULL(dr.Role_Key,-1) as Role_Key "
    ,"       , IFNULL(dw.Warehouse_Key,-1) as Warehouse_Key "
    ,"       , IFNULL(dwt.Warehouse_Type_Key,-1) as Warehouse_Type_Key "
    ,"       , IFNULL(dws.Warehouse_Size_Key,-1) as Warehouse_Size_Key "
    ,"       , IFNULL(des.Execution_Status_Key,-1) as Execution_Status_Key "
    ,"       , IFNULL(de.Error_Key,-1) as Error_Key "
    ,"       , sdd.Date_Key as Start_Date_Key "
    ,"       , edd.Date_Key as End_Date_Key "
    ,"       from Query_History qh "
    ,"       Left join "
    , SCHEMA
    , ".Dim_Session dss on qh.Session_ID = dss.Session_ID "
    ,"       left join "
    , SCHEMA
    , ".Dim_Query_Details qd on qh.Query_Text = qd.Query_Text "
    ,"       left join "
    , SCHEMA
    , ".Dim_Database as ddb on qh.Database_Name = ddb.Database_Name "
    ,"       left join "
    , SCHEMA
    , ".Dim_Schema ds on qh.Schema_Name = ds.Schema_Name "
    ,"       left join "
    , SCHEMA
    , ".dim_Query_Type dqt on qh.Query_Type = dqt.Query_Type "
    ,"       left join "
    , SCHEMA
    , ".dim_User du on qh.User_Name = du.User_Name "
    ,"       left join "
    , SCHEMA
    , ".dim_Role dr on qh.Role_Name = dr.Role_Name "
    ,"       left join "
    , SCHEMA
    , ".dim_Warehouse dw on qh.Warehouse_Name = dw.Warehouse_Name "
    ,"       left join "
    , SCHEMA
    , ".dim_Warehouse_Size dws on qh.Warehouse_Size = dws.Warehouse_Size "
    ,"       left join "
    , SCHEMA
    , ".dim_Warehouse_Type dwt on qh.Warehouse_Type = dwt.Warehouse_Type "
    ,"       Left join "
    , SCHEMA
    , ".dim_Execution_Status des on qh.Execution_Status = des.Execution_Status  "
    ,"       Left join "
    , SCHEMA
    , ".dim_Query_Error de on qh.Query_Id = de.Query_Id "
    ,"                                                and qh.Error_Code = de.Error_Code "
    ,"       left join QueryHistory.Dim_Date as sdd on qh.Start_Date = sdd.Date "
    ,"       left join QueryHistory.Dim_Date as edd on qh.End_Date = edd.Date) as Source "
    ," ON TARGET.Session_Key = Source.Session_Key "
    ," AND Target.Query_ID = Source.Query_ID "
    ," WHEN NOT MATCHED  "
    ," THEN INSERT (Query_ID "
    ,"              , Cluster_Number  "
    ,"              , Query_Tag  "
    ,"              , Start_Time  "
    ,"              , End_Time  "
    ,"              , TOTAL_ELAPSED_TIME  "
    ,"              , BYTES_SCANNED  "
    ,"              , ROWS_PRODUCED "
    ,"              , COMPILATION_TIME " 
    ,"              , EXECUTION_TIME  "
    ,"              , QUEUED_PROVISIONING_TIME  "
    ,"              , QUEUED_REPAIR_TIME "
    ,"              , QUEUED_OVERLOAD_TIME " 
    ,"              , TRANSACTION_BLOCKED_TIME  "
    ,"              , OUTBOUND_DATA_TRANSFER_CLOUD  "
    ,"              , OUTBOUND_DATA_TRANSFER_REGION  "
    ,"              , OUTBOUND_DATA_TRANSFER_BYTES  "
    ,"              , INBOUND_DATA_TRANSFER_CLOUD  "
    ,"              , INBOUND_DATA_TRANSFER_REGION  "
    ,"              , INBOUND_DATA_TRANSFER_BYTES  "
    ,"              , Query_Detail_Key  "
    ,"              , Session_Key  "
    ,"              , Database_Key  "
    ,"              , Schema_Key  "
    ,"              , Query_Type_Key  "
    ,"              , User_Key  "
    ,"              , Role_Key  "
    ,"              , Warehouse_Key  "
    ,"              , Warehouse_Size_Key  "
    ,"              , Warehouse_Type_Key  "
    ,"              , Execution_Status_Key  "
    ,"              , Error_Key  "
    ,"              , Start_Date_Key "
    ,"              , End_Date_Key) "
    ," Values (Source.Query_ID "
    ,"          , Source.Cluster_Number  "
    ,"          , Source.Query_Tag  "
    ,"          , Source.Start_Time  "
    ,"          , Source.End_Time  "
    ,"          , Source.TOTAL_ELAPSED_TIME  "
    ,"          , Source.BYTES_SCANNED  "
    ,"          , Source.ROWS_PRODUCED "
    ,"          , Source.COMPILATION_TIME " 
    ,"          , Source.EXECUTION_TIME  "
    ,"          , Source.QUEUED_PROVISIONING_TIME  "
    ,"          , Source.QUEUED_REPAIR_TIME "
    ,"          , Source.QUEUED_OVERLOAD_TIME " 
    ,"          , Source.TRANSACTION_BLOCKED_TIME  "
    ,"          , Source.OUTBOUND_DATA_TRANSFER_CLOUD  "
    ,"          , Source.OUTBOUND_DATA_TRANSFER_REGION  "
    ,"          , Source.OUTBOUND_DATA_TRANSFER_BYTES  "
    ,"          , Source.INBOUND_DATA_TRANSFER_CLOUD  "
    ,"          , Source.INBOUND_DATA_TRANSFER_REGION  "
    ,"          , Source.INBOUND_DATA_TRANSFER_BYTES  "
    ,"          , Source.Query_Detail_Key  "
    ,"          , Source.Session_Key  "
    ,"          , Source.Database_Key  "
    ,"          , Source.Schema_Key  "
    ,"          , Source.Query_Type_Key  "
    ,"          , Source.User_Key  "
    ,"          , Source.Role_Key  "
    ,"          , Source.Warehouse_Key  "
    ,"          , Source.Warehouse_Size_Key  "
    ,"          , Source.Warehouse_Type_Key  "
    ,"          , Source.Execution_Status_Key  "
    ,"          , Source.Error_Key  "
    ,"          , Source.Start_Date_Key "
    ,"          , Source.End_Date_Key);  "])

    SESSION.execute(queryText)

###############    Merge Dim Execution Status  #########################
def Create_Query_History_Stage_Table(SESSION):
    #Create Temp table to hold all history data from date range above
    queryText = ''.join(["CREATE OR REPLACE TEMPORARY TABLE Query_History ( "
    ," Query_ID String "
    ," , Query_Text String "
    ," , Database_Name String "
    ," , Schema_Name String "
    ," , Query_Type String "
    ," , Session_ID number "
    ," , User_Name String "
    ," , Role_Name String "
    ," , Warehouse_Name String "
    ," , Warehouse_Size String "
    ," , Warehouse_Type String "
    ," , Cluster_Number Number "
    ," , Query_Tag String "
    ," , Execution_Status String "
    ," , Error_Code number "
    ," , Error_Message String "
    ," , Start_Time timestamp "
    ," , End_Time timestamp "
    ," , TOTAL_ELAPSED_TIME NUMBER "
    ," , BYTES_SCANNED NUMBER "
    ," , ROWS_PRODUCED NUMBER "
    ," , COMPILATION_TIME NUMBER "
    ," , EXECUTION_TIME NUMBER "
    ," , QUEUED_PROVISIONING_TIME NUMBER "
    ," , QUEUED_REPAIR_TIME	NUMBER "
    ," , QUEUED_OVERLOAD_TIME NUMBER "
    ," , TRANSACTION_BLOCKED_TIME NUMBER "
    ," , OUTBOUND_DATA_TRANSFER_CLOUD TEXT "
    ," , OUTBOUND_DATA_TRANSFER_REGION TEXT "
    ," , OUTBOUND_DATA_TRANSFER_BYTES NUMBER "
    ," , INBOUND_DATA_TRANSFER_CLOUD TEXT "
    ," , INBOUND_DATA_TRANSFER_REGION TEXT "
    ," , INBOUND_DATA_TRANSFER_BYTES NUMBER "
    ," , Start_Date Date "
    ," , Start_TS Time "
    ," , End_Date Date "
    ," , End_TS Time "
    ," ) "
    ," as  " 
    ," Select Query_ID  "
    ,"     , Query_Text  "
    ,"     , Database_Name " 
    ,"     , Schema_Name  "
    ,"     , Query_Type  "
    ,"     , Session_ID  "
    ,"     , User_Name "
    ,"     , Role_Name  "
    ,"     , Warehouse_Name  "
    ,"     , Warehouse_Size  "
    ,"     , Warehouse_Type  "
    ,"     , Cluster_Number  "
    ,"     , Query_Tag  "
    ,"     , Execution_Status  "
    ,"     , Error_Code  "
    ,"     , Error_Message " 
    ,"     , Start_Time::DateTime "
    ,"     , End_Time::DateTime "
    ,"     , TOTAL_ELAPSED_TIME  "
    ,"     , BYTES_SCANNED  "
    ,"     , ROWS_PRODUCED  "
    ,"     , COMPILATION_TIME " 
    ,"     , EXECUTION_TIME  "
    ,"     , QUEUED_PROVISIONING_TIME  "
    ,"     , QUEUED_REPAIR_TIME	 "
    ,"     , QUEUED_OVERLOAD_TIME  "
    ,"     , TRANSACTION_BLOCKED_TIME " 
    ,"     , OUTBOUND_DATA_TRANSFER_CLOUD  "
    ,"     , OUTBOUND_DATA_TRANSFER_REGION  "
    ,"     , OUTBOUND_DATA_TRANSFER_BYTES  "
    ,"     , INBOUND_DATA_TRANSFER_CLOUD  "
    ,"     , INBOUND_DATA_TRANSFER_REGION  "
    ,"     , INBOUND_DATA_TRANSFER_BYTES  "
    ,"     , Start_Time::Date as Start_Date "
    ,"     , Start_Time::Time as Start_TS "
    ,"     , End_Time::Date as End_Date "
    ,"     , End_Time::Time as End_TS "
    ," from table(information_schema.query_history( "
    ," end_time_range_start => to_timestamp_ltz($range_Start), "
    ," end_time_range_end => to_timestamp_ltz($range_End), "
    ," RESULT_LIMIT => 10000 "
    ," )) "
    ," ORDER BY start_time desc "
    ," , Query_Text ASC "
    ," , TOTAL_ELAPSED_TIME ASC;  "])

    SESSION.execute(queryText)


