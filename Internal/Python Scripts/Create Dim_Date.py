################Import Libraies############################

import time
import datetime
from datetime import date
from datetime import timedelta

###########################################################
################Define Functions###########################
###########################################################

################lastDayofMonth#############################
def lastDayofMonth(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this is to offset for Feb
    return next_month - datetime.timedelta(days=next_month.day)

################createDateValues###########################
def createDateValues(start_Year, End_Year):
    startYear = start_Year
    endYear = End_Year
    Year = 0
    Month = 0
    Quarter = 0 

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
        LastDayofMonth = lastDayofMonth(newDate).strftime("%m/%d/%Y")
        FirstDayofYear = newDate.strftime("%Y-01-01") 
        LastDayofYear = newDate.strftime("%Y-12-31") 
        DayOfYear = DayOfYear + 1


        #Quarter values
        if(newDate.month == 1 or newDate.month == 2 or newDate.month == 3):
            Quarter = 1
            QuarterName = 'First'
            FirstDayofQuarter = newDate.strftime("%Y-01-01")
            LastDayOfQuarter = newDate.strftime("%Y-03/31")

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
        
        
        print(Date_Key, Date, FullDateDDMM, FullDateMMDD, DayOfMonth, DaySuffix, DayName, DayOfWeek, DayOfWeekInMonth, DayOfWeekInYear, DayofWeekInQuarter, DayOfYear
        , WeekofMonth, WeekofQuarter, WeekofYear, Month, MonthName, MonthOfQuarter, Quarter, QuarterName, Year, YearName, MonthYear, MMYYY, FirstDayofMonth, LastDayofMonth
        , FirstDayofQuarter, LastDayOfQuarter, FirstDayofYear, LastDayofYear, IsWeekDay)



###########################################################
###################Execute Process#########################
createDateValues(1975, 1976)