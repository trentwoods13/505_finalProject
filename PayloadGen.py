import random
import names
import uuid
import json

zipList = []
patientCode = ["0", "1", "2", "3", "4", "5", "6"]

def init():
    zipcodesFile = 'kyzipdetails.csv'

    file1 = open(zipcodesFile, 'r')
    kyZipCodes = file1.readlines()

    count = 0
    # Strips the newline character
    for zipLine in kyZipCodes:
        # print(zipLine)
        if (count > 0):
            zipLine = zipLine.strip()
            zipSplit = zipLine.split(",")
            zipList.append(zipSplit[0])
        count = count + 1

    #for st in zipList:
    #    print(st)

def getrandpayload():
    count = random.randint(1, 10)
    return getpayload(count)

def getpayload(count):

    patientList = []
    for i in range(count):
        patientList.append(getperson())

    return json.dumps(patientList)



def getperson():

    first_name = names.get_first_name()
    last_name = names.get_last_name()
    mrn = str(uuid.uuid1())
    zip_code = random.choice(zipList)
    patient_status_code = random.choice(patientCode)

    #print(first_name)
    #print(last_name)
    #print(mrn)
    #print(zip_code)
    #print(patient_status_code)
    patientRecord = dict()
    patientRecord["first_name"] = first_name
    patientRecord["last_name"] = last_name
    patientRecord["mrn"] = mrn
    patientRecord["zip_code"] = zip_code
    patientRecord["patient_status_code"] = patient_status_code

    return patientRecord
