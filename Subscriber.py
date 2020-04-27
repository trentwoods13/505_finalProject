#!/usr/bin/env python
import pika
import sys
import json
from DBTools import loadDB, insertPatient, increaseCount, decreaseCount
import pyorient
import csv
import time
# Set the connection parameters to connect to rabbit-server1 on port 5672
# on the / virtual host using the username "guest" and password "guest"

start = time.time()
multiplier = 1
zip_Positive_copy = {}

def init():
    #attempts to create database
    #if succeeds: starts listening to message broker
    #if failes: catches exeption and does not start listening
    loadCheck = loadDB()

    if loadCheck == True:
        global status
        status = 1
        #database name
        dbname = "covid_reporting_project"
        #database login is root by default
        login = "root"
        #database password, set by docker param
        password = "rootpwd"
        #create client to connect to local orientdb docker container
        client = pyorient.OrientDB("172.31.144.183", 2424)
        session_id = client.connect(login, password)
        client.db_open(dbname, login, password)

        #connect to RabbitMQ message publisher
        username = 'student'
        password = 'student01'
        hostname = '128.163.202.61'
        virtualhost = 'patient_feed'

        credentials = pika.PlainCredentials(username, password)
        parameters = pika.ConnectionParameters(hostname,
                                               5672,
                                               virtualhost,
                                               credentials)

        connection = pika.BlockingConnection(parameters)

        channel = connection.channel()

        exchange_name = 'patient_data'
        channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

        result = channel.queue_declare('', exclusive=True)
        queue_name = result.method.queue

        binding_keys = "#"

        if not binding_keys:
            sys.stderr.write("Usage: %s [binding_key]...\n" % sys.argv[0])
            sys.exit(1)

        for binding_key in binding_keys:
            channel.queue_bind(
                    exchange=exchange_name, queue=queue_name, routing_key=binding_key)

        filepath = "hospitals.csv"
        file =  open(filepath)
        readFile = csv.reader(file, delimiter = ',')
        headerInfo = next(readFile)
        allHospitals = []
        lvlFourHospitals = []
        for row in readFile:
            #append hosital IDs and zip codes
            allHospitals.append([row[0], row[5]])
            if row[16] == 'LEVEL IV':
                lvlFourHospitals.append([row[0], row[5]])

        print("Importing zip code distances...")
        filepath = "kyzipdistance.csv"
        file =  open(filepath)
        readFile = csv.reader(file, delimiter = ',')
        headerInfo = next(readFile)
        rowCount = 0
        zipDict = {}
        # create additional zip dictionary with # of positive cases
        zip_Positive = {}

        for row in readFile:
            # all initally have 0 positive cases
            zip_Positive[row[0]] = 0

            key = (row[0], row[1])
            zipDict[key] = row[2]
            rowCount += 1

        #for key,value in zip_Positive.items():
            #client.command("CREATE VERTEX Zip SET Code = " + key + ", Positive = 0")

        print('[*] Waiting for logs. To exit press CTRL+C')


        def callback(ch, method, properties, body):
            global start
            global multiplier
            global zip_Positive_copy

            data = json.loads(body)
            for patient in data:
                #print(patient)
                for key in patient:
                    first_name = patient["first_name"]
                    last_name = patient["last_name"]
                    mrn = patient["mrn"]
                    zip_code = patient["zip_code"]
                    patient_status_code = patient["patient_status_code"]

                    #ensure that only unique patients are inserted
                    query = "SELECT COUNT(*) FROM Patient WHERE mrn = '" + mrn + "'"
                    response = client.command(query)


                    alert_list = []

                    if response[0].COUNT == 0:

                        #insert patient into database
                        insertPatient(first_name, last_name, mrn, zip_code, patient_status_code, client)

                        # if for some reason zip code wasn't added beforehand, initialize it
                        if zip_code not in zip_Positive.keys():
                            zip_Positive[zip_code] = 0
                            #client.command("CREATE VERTEX Zip SET Code = " + key + ", Positive = 0")


                        # for positive and negative testing counts
                        if(patient_status_code == '2' or patient_status_code == '5' or patient_status_code == '6'):
                            increaseCount(client)
                            if time.time() > start + 15:

                                # the following lines can be used to test the real-time alert
                                #zip_code1 = '41139'
                                #zip_code2 = '40504'
                                #zip_code3 = '41101'
                                #zip_code4 = '40508'
                                #zip_code5 = '41102'
                                #x = zip_Positive[zip_code1] + 5
                                #zip_Positive[zip_code1] = x
                                #x = zip_Positive[zip_code2] + 5
                                #zip_Positive[zip_code2] = x
                                #x = zip_Positive[zip_code3] + 5
                                #zip_Positive[zip_code3] = x
                                #x = zip_Positive[zip_code4] + 5
                                #zip_Positive[zip_code4] = x
                                #x = zip_Positive[zip_code5] + 5
                                #zip_Positive[zip_code5] = x
                            #else:
                                x = zip_Positive[zip_code] + 1
                                zip_Positive[zip_code] = x
                        else:
                            decreaseCount(client)

                        # every 15 seconds create a copy of the dict to compare to previous 15 seconds
                        if time.time() > start + multiplier * 15:
                            if multiplier == 1:
                                zip_Positive_copy = zip_Positive.copy()
                                multiplier = multiplier + 1
                            else:
                                multiplier = multiplier + 1
                                # loop through the common zip codes
                                for key in zip_Positive.keys() & zip_Positive_copy.keys():
                                    if zip_Positive_copy[key] == 0:
                                        continue
                                    else:
                                        # compare value now to value 15 seconds ago
                                        if zip_Positive[key] >= 2 * zip_Positive_copy[key]:
                                            alert_list.append(key)

                                # send list of zip codes to DB
                                print(alert_list)
                                #x = ''
                                #if len(alert_list) == 0:
                                #    x = 'none'
                                #else:
                                #    for zip in alert_list:
                                #        x = x + zip + ' '

                                client.command("UPDATE Alert_list SET Zip =" + str(alert_list))
                                zip_Positive_copy = zip_Positive.copy()



                        #if the patient needs any level of hospital care route to nearest hospital of any level
                        if(patient_status_code == '3' or patient_status_code == '5'):
                            #route to closest available hospital
                            distances = []
                            for i in range(len(allHospitals)):
                                key = (zip_code, allHospitals[i][1])
                                if key in zipDict:
                                    distances.append([allHospitals[i][0], zipDict.get(key)])
                                else:
                                    return
                            #find the closeset hospitals
                            distances.sort(key = lambda x: x[1])
                            #find databse id of closest hospital
                            nodeIDHospital = -1
                            for i in range(len(distances)):
                                if nodeIDHospital == -1:
                                    query = "SELECT FROM Hospital WHERE ID = '" + str(distances[i][0]) + "'"
                                    response = client.command(query)
                                    bedCount = response[0].Available_beds
                                    if bedCount > 0:
                                        nodeIDHospital = response[0]._rid
                            if nodeIDHospital != -1:
                                #find database id of patient
                                getPatientID = "SELECT FROM Patient WHERE mrn = '" + str(mrn) + "'"
                                nodePatient = client.query(getPatientID)

                                if str(nodePatient[0]._rid) == '':
                                    break
                                else:
                                    nodeIDPatient = str(nodePatient[0]._rid)
                                    #connect patient to hospital in database
                                    makeEdge = "CREATE EDGE FROM " + nodeIDPatient + " TO " + nodeIDHospital
                                    client.command(makeEdge)
                                    #remove a bed from the hospital
                                    updateBeds = "UPDATE " + nodeIDHospital + " INCREMENT Available_beds = -1"
                                    client.command(updateBeds)
                            else:
                                print("No hospitals with available beds for this patient")

                        #if the patient needs emergency hospital care route them to nearest level 4 hospital
                        elif(patient_status_code == '6'):
                            #route to closest available level 4 hospital
                            distances = []
                            for i in range(len(lvlFourHospitals)):
                                key = (zip_code, allHospitals[i][1])
                                if key in zipDict:
                                    distances.append([allHospitals[i][0], zipDict[key]])
                                else:
                                    return
                            #find the closeset hospitals
                            distances.sort(key = lambda x: x[1])
                            #find databse id of closest hospital
                            nodeIDHospital = -1
                            for i in range(len(distances)):
                                if nodeIDHospital == -1:
                                    query = "SELECT FROM Hospital WHERE ID = '" + str(distances[i][0]) + "'"
                                    response = client.command(query)
                                    bedCount = response[0].Available_beds
                                    if bedCount > 0:
                                        nodeIDHospital = response[0]._rid
                            if nodeIDHospital != -1:
                                #find database id of patient
                                getPatientID = "SELECT FROM Patient WHERE mrn = '" + str(mrn) + "'"

                                nodePatient = client.query(getPatientID)

                                if str(nodePatient[0]._rid) == '':
                                    break
                                else:
                                    nodeIDPatient = str(nodePatient[0]._rid)
                                    #connect patient to hospital in database
                                    makeEdge = "CREATE EDGE FROM " + nodeIDPatient + " TO " + nodeIDHospital
                                    client.command(makeEdge)
                                    #remove a bed from the hospital
                                    updateBeds = "UPDATE " + nodeIDHospital + " INCREMENT Available_beds = -1"
                                    client.command(updateBeds)
                            else:
                                print("No hospitals with available beds for this patient")

        channel.basic_consume(
                queue=queue_name, on_message_callback=callback, auto_ack=True)

        channel.start_consuming()
    else:
        print("Error connecting to database")

init()
