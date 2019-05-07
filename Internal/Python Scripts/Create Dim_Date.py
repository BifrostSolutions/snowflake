import time
from datetime import date
from datetime import timedelta

startYear = 1975
endYear = 1980


d0 = date(startYear, 1, 1)
d1 = date(endYear, 1, 1)
delta = d1 - d0
print (delta.days)

#timedelta(days=1)
for i in range(0,delta.days):
    newDate = d0 + timedelta(days=i)
    print(newDate.strftime("%Y%m%d") #Date_Key
    , newDate.strftime("%Y-%m-%d") #Date
    , newDate.strftime("%d/%m/%Y") #FullDateDDMM
    , newDate.strftime("%m/%d/%Y") #FullDateMMDD
    , newDate.day #DayOfMonth
    , newDate.weekday()
    , newDate.
    )
