

import sys
sys.path.append('/usr/lib/python2.7/dist-packages')
import csv
from pysqlite2 import dbapi2 as sqlite3
#con = sqlite3.connect(":memory:")
con = sqlite3.connect('dabase.db')
con.enable_load_extension(True)
con.load_extension("/home/vagrant/libsqlitefunctions")

con.text_factory = str
c = con.cursor()  # create cursor object and call its execute method to perform sql command


for row in c.execute('SELECT * FROM actions'):
        print row


for row in c.execute('SELECT * FROM cuisines'):
        print row



for row in c.execute('SELECT * FROM violations'):
    print row


# ### CAMIS,DBA,BORO,BUILDING,STREET,ZIPCODE,PHONE,CUISINECODE,INSPDATE,ACTION,VIOLCODE, SCORE,CURRENTGRADE,GRADEDATE,RECORDDATE

# #Score by ZipCode


# Print Column Name of the Table Grades
cursor = con.execute('select * from grades')
names = [description[0] for description in cursor.description]
print names



# Create a NEW TABLE
con.execute('DROP TABLE IF EXISTS score_table')
con.execute('CREATE TABLE score_table AS SELECT ZIPCODE,SCORE,VIOLCODE FROM grades ORDER BY ZIPCODE')


con.enable_load_extension(False)
con.execute('UPDATE score_table SET VIOLCODE=1')
con.execute('CREATE TABLE final_score_table(ZIPCODE, MEAN, STANDARD_ERROR, TOTOAL_VIOLATION)')


for row in con.execute('SELECT ZIPCODE, AVG(SCORE),STDEV(SCORE)/SQRT(COUNT(*)),SUM(VIOLCODE) FROM score_table GROUP BY ZIPCODE'):
    con.execute('INSERT INTO final_score_table VALUES (?,?,?,?)',row)

s = 0
List_score = []

for row in con.execute('SELECT * FROM final_score_table WHERE TOTOAL_VIOLATION>100'):
    s+=1
    List_score.append(row)
print s
print List_score



# # Score_by_Map
con.execute('DROP TABLE IF EXISTS map')
con.execute('CREATE TABLE map AS SELECT ZIPCODE, MEAN FROM final_score_table')

con.execute('DELETE FROM map WHERE MEAN = 0.0')
for row in con.execute('SELECT * FROM map'):
    print row


import csv
with open('map.csv', 'wb') as csvfile:
    for row in con.execute('SELECT * FROM map'):
        w = csv.writer(csvfile, delimiter=',')
        L = list(row)
        w.writerow(L)


# #### URL:¡¡http://cdb.io/1UN3K1w

# # score_by_borough

con.execute('DROP TABLE IF EXISTS score_borough')
con.execute('CREATE TABLE score_borough AS SELECT BORO,SCORE,VIOLCODE FROM grades ORDER BY SCORE ASC')


con.enable_load_extension(False)
con.execute('DROP TABLE IF EXISTS final_score_borough')
con.execute('UPDATE score_borough SET VIOLCODE=1')
con.execute('CREATE TABLE final_score_borough(BORO, MEAN, STANDARD_ERROR, TOTOAL_VIOLATION)')


for row in con.execute('SELECT BORO, AVG(SCORE),STDEV(SCORE)/SQRT(COUNT(*)),COUNT(*) FROM score_borough GROUP BY BORO'):
    con.execute('INSERT INTO final_score_borough VALUES (?,?,?,?)',row)



boro_score = []
con.execute('DELETE FROM final_score_borough WHERE BORO="0"')
for row in con.execute('SELECT * FROM final_score_borough'):
    boro_score.append(row)
print boro_score


# # Score_by_cuisine


con.execute('DROP TABLE IF EXISTS score_cuisine')
con.execute('CREATE TABLE score_cuisine AS SELECT CUISINECODE,SCORE,VIOLCODE FROM grades ORDER BY SCORE ASC')



con.enable_load_extension(False)
con.execute('DROP TABLE IF EXISTS final_score_cuisine')
con.execute('CREATE TABLE final_score_cuisine(CUISINECODE, MEAN, STANDARD_ERROR, TOTOAL_VIOLATION)')



for row in con.execute('SELECT CUISINECODE, AVG(SCORE),STDEV(SCORE)/SQRT(COUNT(*)),COUNT(*) FROM score_cuisine GROUP BY CUISINECODE'):
    con.execute('INSERT INTO final_score_cuisine VALUES (?,?,?,?)',row)


cuisine_score = []
input_file = csv.DictReader(open('Cuisine.txt'))
dic={}
for row in input_file:
    dic[row["CUISINECODE"]] = str(row["CODEDESC"])
for row in con.execute('SELECT * FROM final_score_cuisine WHERE TOTOAL_VIOLATION>=100 '):
    cuisine_score.append(list(row))
#print cuisine_score

new_cuisine_score = []
for item in cuisine_score:
    item[0] = dic[item[0]]
    new_cuisine_score.append(tuple(item))
print new_cuisine_score
print len(new_cuisine_score)


# # Violation_by_cuisine


cursor = con.execute('select * from grades')
names = [description[0] for description in cursor.description]
print names



cursor = con.execute('select * from violations')
names = [description[0] for description in cursor.description]
print names


con.execute('DROP TABLE IF EXISTS violation_endate')
con.execute("""CREATE TABLE violation_endate AS SELECT A.CUISINECODE AS CUISINECODE, A.VIOLCODE AS VIOLCODE ,B.ENDDATE AS ENDDATE
  ,A.COUNT0 AS COUNT0     FROM( SELECT CUISINECODE, VIOLCODE, COUNT(*) AS COUNT0 From grades
        Group By CUISINECODE, VIOLCODE
) AS A
INNER JOIN (SELECT VIOLATIONCODE,ENDDATE FROM violations) AS B
 ON A.VIOLCODE = B.VIOLATIONCODE
WHERE ENDDATE>'2014-01-1 00:00:00'
""")
for row in con.execute('SELECT * FROM violation_endate'):
    print row



s = 0
con.execute('DROP TABLE IF EXISTS cond_prob')
con.execute('''CREATE TABLE cond_prob AS SELECT  A.CUISINECODE AS CUISINECODE, A.VIOLCODE AS VIOLCODE , A.COUNT0 * 1.0 / B.COUNT2 As Freq
,A.COUNT0 AS COUNT0 From    (
        Select CUISINECODE, VIOLCODE,COUNT0 
        From   violation_endate 
        Group By CUISINECODE, VIOLCODE
        ) As A
        Inner Join (
            Select CUISINECODE, COUNT(*) As COUNT2
            From   grades 
            Group By CUISINECODE
            ) As B
            On A.CUISINECODE = B.CUISINECODE
           ''')
for row in con.execute('SELECT * FROM cond_prob'):
    print row


con.execute('DROP TABLE IF EXISTS new_grades')
con.execute("""CREATE TABLE new_grades AS SELECT A.CUISINECODE AS CUISINECODE, A.VIOLCODE AS VIOLCODE ,B.ENDDATE AS ENDDATE
        FROM( SELECT CUISINECODE, VIOLCODE FROM grades) AS A
        INNER JOIN (SELECT VIOLATIONCODE,ENDDATE FROM violations) AS B
        ON A.VIOLCODE = B.VIOLATIONCODE
        WHERE ENDDATE>'2014-01-1 00:00:00'
""")

for row in con.execute('SELECT * FROM new_grades'):
    print row



con.execute('DROP TABLE IF EXISTS vio_table')
con.execute('''CREATE TABLE vio_table AS Select A.VIOLCODE AS VIOLCODE,A.COUNT3 AS COUNT3, B.vio_count As TOTAL
From    (
        Select VIOLCODE,COUNT(*) AS COUNT3
        From   new_grades 
        Group By VIOLCODE
        ) As A
        Inner Join (
            Select Count(*) As vio_count
            From   new_grades 
            ) As B
           ''')
con.execute('DROP TABLE IF EXISTS prob_vio')
con.execute('CREATE TABLE prob_vio AS SELECT VIOLCODE,COUNT3*1.0/TOTAL AS Prob_vio FROM vio_table GROUP BY VIOLCODE')
for row in con.execute('SELECT * FROM prob_vio'):
    print row
    


# CREATE THE FINAL VERSION OF THE TABLE
con.execute('DROP TABLE IF EXISTS final_table')
con.execute('''CREATE TABLE final_table AS Select A.CUISINECODE AS CUISINECODE, B.VIOLCODE AS VIOLCODE, A.Freq/B.Prob_vio As RATIO
, A.COUNT0 AS COUNT0 From    (
        Select CUISINECODE,VIOLCODE,Freq,COUNT0 
        From   cond_prob 
        WHERE COUNT0>100) As A
        Inner Join (
            Select VIOLCODE, Prob_vio
            From   prob_vio 
            ) As B
            On A.VIOLCODE = B.VIOLCODE
           ''')


import re
FINAL_LIST = []
cuisine_dict = {}
input_cuisine_file = csv.DictReader(open('Cuisine.txt'))
for row in input_cuisine_file:
    cuisine_dict[row["CUISINECODE"]] = str(row["CODEDESC"]).decode("ascii","ignore")

input_violation_file = csv.DictReader(open('Violation.txt'))
for row in input_violation_file:
    violation_dict[row["VIOLATIONCODE"]] = str(row["VIOLATIONDESC"]).decode("ascii","ignore")
#print violation_dict


for row in con.execute('SELECT * FROM final_table ORDER BY RATIO DESC LIMIT 20'):
    FINAL_LIST.append(list((list((row[0],row[1])),row[2],row[3])))
print FINAL_LIST

final_cuisine_violation = []
for item in FINAL_LIST:
    item[0][0] = str(cuisine_dict[item[0][0]])
    item[0][1] = str(violation_dict[item[0][1]])
    new_item = ((item[0][0],item[0][1]),item[1],item[2])
    final_cuisine_violation.append(new_item)
print final_cuisine_violation
print len(final_cuisine_violation)
    

con.commit()
con.close()


