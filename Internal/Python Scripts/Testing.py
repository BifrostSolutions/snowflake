#Load File
f = open("C:\\Users\\cirwin\\OneDrive - Teknion Data Solutions\\Projects\snowflake\\Internal\\QueryFile.txt", 'r')
content = f.read()
Queries = content.split(';')
print(len(Queries))
#print(content)
f.close()



for queryToRun in Queries:
    #cs.execute(queryToRun + ';')
    print(queryToRun + ';')