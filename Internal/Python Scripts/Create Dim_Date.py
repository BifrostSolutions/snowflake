import time
import datetime
from datetime import date
from datetime import timedelta
import pandas as pd

################Define Functions#######################
def last_day_of_month(any_day):
    next_month = any_day.replace(day=28) + datetime.timedelta(days=4)  # this will never fail
    return next_month - datetime.timedelta(days=next_month.day)




startYear = 1975
endYear = 1976



d0 = date(startYear, 1, 1)
d1 = date(endYear, 1, 1)
delta = d1 - d0
print (delta.days)


dateValues = pd.DataFrame(columns = ('Date_Key','Date','FullDateDDMM' ,'FullDateMMDD','DayOfMonth','DaySuffix' ,'DayName' ,'DayOfWeek' ,'DayOfWeekInMonth'
,'DayOfWeekInYear' ,'DayOfQuarter','DayOfYear' ,'WeekOfMonth','WeekOfQuarter','WeekOfYear' ,'Month','MonthName','MonthOfQuarter','Quarter' ,'QuarterName','Year' 			
,'YearName' ,'MonthYear' ,'MMYYYY' ,'FirstDayOfMonth','LastDayOfMonth' ,'FirstDayOfQuarter','LastDayOfQuarter','FirstDayOfYear' ,'LastDayOfYear' ,'IsHoliday','IsWeekday' ))


for i in range(0,delta.days):
    newDate = d0 + timedelta(days=i)
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
    LastDayofMonth = last_day_of_month(newDate).strftime("%m/%d/%Y")
    FirstDayofYear = newDate.strftime("01/01/%Y") 
    LastDayofYear = newDate.strftime("12/31/%Y") 

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
    elif(DayOfWeek == 2):
        DayName = 'Monday'
    elif(DayOfWeek == 3):
        DayName = 'Tuesday'
    elif(DayOfWeek == 4):
        DayName = 'Wednesday'
    elif(DayOfWeek == 5):
        DayName = 'Thrusday'
    elif(DayOfWeek == 6):
        DayName = 'Friday'
    elif(DayOfWeek == 7):
        DayName = 'Saturday'

    #Month Name
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

    #Quarter values
    if(newDate.month == 1 or newDate.month == 2 or newDate.month == 3):
        Quarter = 1
        QuarterName = 'First'
        FirstDayofQuarter = newDate.strftime("01/01/%Y")
        LastDayOfQuarter = newDate.strftime("03/31/%Y")
    elif(newDate.month == 4 or newDate.month == 5 or newDate.month == 6):
        Quarter = 2
        QuarterName = 'Second'
        FirstDayofQuarter = newDate.strftime("04/01/%Y")
        LastDayOfQuarter = newDate.strftime("06/30/%Y")
    elif(newDate.month == 7 or newDate.month == 8 or newDate.month == 9):
        Quarter = 3
        QuarterName = 'Third'
        FirstDayofQuarter = newDate.strftime("07/01/%Y")
        LastDayOfQuarter = newDate.strftime("09/30/%Y")
    elif(newDate.month == 10 or newDate.month == 11 or newDate.month == 12):
        Quarter = 4
        QuarterName = 'Fourth'
        FirstDayofQuarter = newDate.strftime("10/01/%Y")
        LastDayOfQuarter = newDate.strftime("12/31/%Y")

    


    
    print(Date_Key, Date, FullDateDDMM, FullDateMMDD, DayOfMonth, DaySuffix, DayName, DayOfWeek
    , Month, MonthName, MonthOfQuarter, Quarter, QuarterName, Year, YearName, MonthYear, MMYYY, FirstDayofMonth, LastDayofMonth
    , FirstDayofQuarter, LastDayOfQuarter, FirstDayofYear, LastDayofYear)


