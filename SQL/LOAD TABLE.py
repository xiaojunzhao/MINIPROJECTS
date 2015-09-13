

import sys
sys.path.append('/usr/lib/python2.7/dist-packages')
import sqlite3 as sql3
import csv
from pysqlite2 import dbapi2 as sqlite3



con = sqlite3.connect(":memory:")
con.enable_load_extension(True)
con.load_extension("/home/vagrant/libsqlitefunctions")
conn = sql3.connect('dabase.db')
conn.text_factory = str
c = conn.cursor()  # create cursor object and call its execute method to perform sql command




#CREATE A ACTION TABLE
input_file = csv.DictReader(open('Action.txt'))
c.execute(''' CREATE TABLE actions
         (STARTDATE,ENDDATE,ACTIONCODE,ACTIONDESC)''')
table = []

for row in input_file:
    item = (row['STARTDATE'],row['ENDDATE'],row['ACTIONCODE'],row['ACTIONDESC'])
    table.append(item)
print table
c.executemany("INSERT INTO actions VALUES (?,?,?,?)",table)
for row in c.execute('SELECT * FROM actions'):
        print row




# Create table Cuisine

input_file_Cuisine = csv.DictReader(open('Cuisine.txt'))
c.execute(''' CREATE TABLE cuisines (CUISINECODE,CODEDESC)''')
table_cuisine = []

for row in input_file_Cuisine:
    item = (row['CUISINECODE'],row['CODEDESC'])
    print item
    table_cuisine.append(item)
#print table_cuisine
c.executemany("INSERT INTO cuisines VALUES (?,?)",table_cuisine)
for row in c.execute('SELECT * FROM cuisines'):
    print row




# create violation table
input_file_Violations = csv.DictReader(open('Violation.txt'))
c.execute('DROP TABLE IF EXISTS violations')
c.execute(''' CREATE TABLE violations (STARTDATE,ENDDATE,CRITICALFLAG,VIOLATIONCODE,VIOLATIONDESC)''')
table_violations = []

for row in input_file_Violations:
    item = (row['STARTDATE'],row['ENDDATE'],row['CRITICALFLAG'],row['VIOLATIONCODE'],row['VIOLATIONDESC'])
    table_violations.append(item)

c.executemany("INSERT INTO violations VALUES (?,?,?,?,?)",table_violations)
#for row in c.execute('SELECT * FROM violations'):
#    print row




# create grades table
input_file_Grades = csv.DictReader(open('WebExtract.txt'))
c.execute(''' CREATE TABLE grades (CAMIS,DBA,BORO,BUILDING,STREET,ZIPCODE,PHONE,CUISINECODE,INSPDATE,ACTION,VIOLCODE,
          SCORE,CURRENTGRADE,GRADEDATE,RECORDDATE)''')
table_grades = []

for row in input_file_Grades:
    item = (row['CAMIS'],row['DBA'],row['BORO'],row['BUILDING'],row['STREET'],           row['ZIPCODE'],row['PHONE'],row['CUISINECODE'],row['INSPDATE'],row['ACTION'],row['VIOLCODE'],           row['SCORE'],row['CURRENTGRADE'],row['GRADEDATE'],row['RECORDDATE'])
    table_grades.append(item)

c.executemany("INSERT INTO grades VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",table_grades)
#for row in c.execute('SELECT * FROM grades'):
#    print row




#create boroughs table
c.execute(''' CREATE TABLE boroughs (MANHATAN,THE BRONX,BROOKLYN,QUEENS,STATEISLAND)''')
#c.execute("INSERT INTO boroughs VALUES (MANHATAN, THE BRONX, BROOKLYN,QUEENS,STATEISLAND)")



conn.commit()
conn.close()




