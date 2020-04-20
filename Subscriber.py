#!/usr/bin/env python
import pika
import sys
import json
from DBTools import loadDB, insertPatient
import pyorient
import csv

# Set the connection parameters to connect to rabbit-server1 on port 5672
# on the / virtual host using the username "guest" and password "guest"

#attempts to create database
#if succeeds: starts listening to message broker
#if failes: catches exeption and does not start listening
loadCheck = loadDB()

if loadCheck == True:
    
    #database name
    dbname = "covid_reporting_project"
    #database login is root by default
    login = "root"
    #database password, set by docker param
    password = "rootpwd"
    #create client to connect to local orientdb docker container
    client = pyorient.OrientDB("172.31.147.250", 2424)
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
    for row in readFile:
        key = (row[0], row[1])
        zipDict[key] = row[2]
        rowCount += 1
    
    print('[*] Waiting for logs. To exit press CTRL+C')


    def callback(ch, method, properties, body):
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
                if response[0].COUNT == 0:
                    
                    #insert patient into database
                    insertPatient(first_name, last_name, mrn, zip_code, patient_status_code, client)
                    
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
                                bedCount = response[0].Beds
                                if bedCount > 0:
                                    nodeIDHospital = response[0]._rid 
                        if nodeIDHospital != -1:
                            #find database id of patient
                            getPatientID = "SELECT FROM Patient WHERE mrn = '" + str(mrn) + "'"
                            nodePatient = client.query(getPatientID)
                            nodeIDPatient = str(nodePatient[0]._rid)
                            #connect patient to hospital in database
                            makeEdge = "CREATE EDGE FROM " + nodeIDPatient + " TO " + nodeIDHospital
                            client.command(makeEdge)
                            #remove a bed from the hospital
                            updateBeds = "UPDATE " + nodeIDHospital + " INCREMENT Beds = -1"
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
                                bedCount = response[0].Beds
                                if bedCount > 0:
                                    nodeIDHospital = response[0]._rid
                        if nodeIDHospital != -1:
                            #find database id of patient
                            getPatientID = "SELECT FROM Patient WHERE mrn = '" + str(mrn) + "'"
                            nodePatient = client.query(getPatientID)
                            nodeIDPatient = str(nodePatient[0]._rid)
                            #connect patient to hospital in database
                            makeEdge = "CREATE EDGE FROM " + nodeIDPatient + " TO " + nodeIDHospital
                            client.command(makeEdge)
                            #remove a bed from the hospital
                            updateBeds = "UPDATE " + nodeIDHospital + " INCREMENT Beds = -1"
                            client.command(updateBeds)
                        else:
                            print("No hospitals with available beds for this patient")
                    
    channel.basic_consume(
            queue=queue_name, on_message_callback=callback, auto_ack=True)
    
    channel.start_consuming()
else:
    print("Error connecting to database")
