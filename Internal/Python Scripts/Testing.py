import TeknionSnowflake

start_Year = 1990

end_Year = 1991

Schema = 'TestingAutomation'

AllDates = TeknionSnowflake.get_Date_Values(start_Year, end_Year)

selectQuery = ''

insertQuery = " Insert into " + Schema + ".Dim_Date Select * from ("

for i in range(0,len(AllDates)):

    if i == 0:

        selectQuery = selectQuery + ' (Select '
    
    else:
        selectQuery = selectQuery + ') Union ALL (Select '

    for date in AllDates[i]:
        
        selectQuery = selectQuery + "'" + str(date) + "', "

    dimDateInsert = (selectQuery[0:(len(selectQuery) -2)] + ')').replace(', )', ')')

insertQuery =  insertQuery + dimDateInsert + ')'
#SESSION.execute(dimDateInsert)
print(insertQuery)