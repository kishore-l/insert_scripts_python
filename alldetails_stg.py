import csv, datetime
import requests, cx_Oracle, json
import os, subprocess
import time

def logInfo(text):
        datenow = datetime.datetime.now()
        print(str(datenow).split('.')[0] + ' - INFO - ' + text)

def logError(text):
        datenow = datetime.datetime.now()
        print(str(datenow).split('.')[0] + ' - ERROR - ' + text)


con = cx_Oracle.connect('STG_EWS','CbEws123','172.16.11.157:1923/ewsdcuat',encoding='UTF-8') # Connecting to the oracle database
cursor = con.cursor()


path = '/cascache/Nexensus/jsons/2023-09-15/'
pkpath = '/cascache/pk/'
json_list = [os.path.join(path, file) for file in os.listdir(path)]
#print(json_list)

#FOR TESTING
try:
    cursor.execute("truncate table STG_EWS.ALLDETAILS_HOSMS_TEST")
    con.commit()
except cx_Oracle.DatabaseError as e:
    logError("Oracle command Failed with following exception ..." ,e)

totalJsons = subprocess.getoutput('ls ' + path + ' | wc -l')
for i in range(int(totalJsons)):
#for i in range(30):
    with open(json_list[i], encoding="utf8") as json_file:
        try:
            data = json.load(json_file)
            required=['cin']
            if all(key in data for key in required):
                pklist = []
                with open(pkpath + 'alldetails.pk', 'r') as f:
                    pklist = [line.strip() for line in f]
                data['lastupdateddate'] = datetime.date.today()
                pk = str(data['cin']) + ':' + str(data['pan'])+ ':' + str(data['isMcaDefaulter'])+ ':' + str(data['isHawala'])+ ':' + str(data['isOffshore'])+ ':' + str(data['status'])
                if pk not in pklist:
                    data['pk']=(str(data['cin']) + ':' + str(data['pan'])+ ':' + str(data['isMcaDefaulter'])+ ':' + str(data['isHawala'])+ ':' + str(data['isOffshore'])+ ':' + str(data['status']))
                    data['total_detail']=(str(data['cin']) + ':' + str(data['pan'])+ ':' + str(data['isMcaDefaulter'])+ ':' + str(data['isHawala'])+ ':' + str(data['isOffshore'])+ ':' + str(data['status']))
                    try:
                        logInfo("Inserting isMcaDefaulter, isHawala, isOffshore, status data for " + json_list[i]  + "..." )
                        cursor.execute("INSERT INTO STG_EWS.ALLDETAILS_HOSMS_TEST (CIN,PAN,NAME,ISMCADEFAULTER, ISHAWALA, ISOFFSHORE, STATUS, LASTUPDATEDDATE,PK,TOTAL_DETAIL) VALUES(:cin, :pan, :name, :isMcaDefaulter, :isHawala, :isOffshore, :status, :lastupdateddate,:pk,substr(standard_hash(:total_detail,'MD5'),1,12))",
                        cin = data['cin'], name=data['name'], pan=data['pan'], isMcaDefaulter = data['isMcaDefaulter'], isHawala = data['isHawala'], isOffshore = data['isOffshore'], status = data['status'], lastupdateddate = data['lastupdateddate'],pk=data['pk'],total_detail=data['total_detail'])
                        con.commit()

                        logInfo("Insert Successfull for " + json_list[i] + ".")

                        pklistappend = open(pkpath + 'alldetails.pk', 'a')
                        pklistappend.write(pk + '\n')
                        pklistappend.close
                    except cx_Oracle.DatabaseError as e:
                        logError("Oracle Insert Failed with following exception for index " + json_list[i] + " .", e)
                        continue
                else:
                    logInfo("Records already inserted for " + json_list[i] + " .")
            else:
                logError("Required keys are missing in the JSON file: " + json_list[i])
        except ValueError:
            logError("Json decode failed " + json_list[i])
            continue
