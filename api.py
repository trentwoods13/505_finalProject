import flask
from flask import jsonify
from flask import request
import pika
import sys
import json
from DBTools import loadDB, insertPatient, increaseCount, decreaseCount
import pyorient
import csv

app = flask.Flask(__name__)
app.config["DEBUG"] = True


status = 1
num_zips_alert = 0

names = {
    "team_name": "Team Flatten the Curve",
    "Team_members_sids": ["10985181, 12182573"],
    "app_status_code": status
    }




@app.route('/', methods=['GET'])
def home():
    return "<h1>CS505 Final Project</h1>"


@app.route('/api/getteam', methods=['GET'])
def returnNames():
    return jsonify(names)

@app.route('/api/testcount', methods=['GET'])
def getTestCount():
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

    # now query the DB
    query = "SELECT * FROM Count"
    response = client.query(query)
    pos_count = response[0].Positive
    neg_count = response[0].Negative

    counts = {

        "positive_test": pos_count,
        "negative_test": neg_count

    }


    return jsonify(counts)


@app.route('/api/reset', methods=['GET'])
def api_reset():

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

    # query
    query = "DELETE VERTEX Patient"
    response_V = client.command(query)

    query1 = "UPDATE Count SET Positive = 0 WHERE Positive != 0"
    client.command(query1)

    query2 = "UPDATE Count SET Negative = 0 WHERE Negative != 0"
    client.command(query2)

    query3 = "UPDATE Alert_list SET Zip = [] WHERE Zip != []"
    client.command(query3)

    query4 = "UPDATE Hospital SET Available_beds = Beds WHERE Available_beds != Beds"
    client.command(query4)

    if response_V[0] != 0:
        reset_status = 1
    else:
        reset_status = -1

    reset = {

        "reset_status_code": reset_status

    }

    return jsonify(reset)


@app.route('/api/zipalertlist', methods=['GET'])

def alert():
    global num_zips_alert
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

    query = "SELECT Zip FROM Alert_list"
    response = client.command(query)
    x = response[0].Zip
    print(x)
    num_zips_alert = len(x)
    print(num_zips_alert)

    zips = {

        'ziplist': x

    }

    return jsonify(zips)


@app.route('/api/alertlist', methods=['GET'])

def alertList():
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


    query = "SELECT Zip FROM Alert_list"
    response = client.command(query)
    x = response[0].Zip
    num_zips_alert = len(x)

    if num_zips_alert >= 5:
        alert_status = 1
    else:
        alert_status = 0

    state = {

        'state_status': alert_status

    }

    return jsonify(state)


@app.route('/api/getpatient/<id>', methods=['GET'])

def getPatient(id):
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

    #mrn = request.args.get('mrn')

    mrn = id
    query = "SELECT * FROM Patient WHERE mrn = '" + mrn + "'"
    nodePatient = client.query(query)

    if nodePatient != []:

        mrn_code = str(nodePatient[0].mrn)

        assignment = nodePatient[0].patient_status_code

        if assignment == 0 or assignment == 1 or assignment == 2 or assignment == 4:
            hospital_code = 0
        else:
            query = "SELECT out().ID FROM PATIENT WHERE mrn = '" + mrn_code + "'"
            hospital = client.query(query)

            hospital_code = str(hospital[0].out)

    else:
        mrn = "not found"
        hospital_code = -1

    returnPatient = {

    "mrn": mrn,
    "location_code": hospital_code

    }
    return jsonify(returnPatient)


@app.route('/api/gethospital/<id>', methods=['GET'])

def getHospital(id):
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

    #hospitalID = request.args.get('ID')
    hospitalID = id

    query = "SELECT * FROM Hospital WHERE ID = " + hospitalID
    nodeHospital = client.query(query)
    print(nodeHospital)

    if nodeHospital != []:

        query = "SELECT Beds FROM HOSPITAL WHERE ID = " + hospitalID
        response = client.query(query)
        print(response)
        beds = response[0].Beds

        query = "SELECT Available_beds FROM HOSPITAL WHERE ID = " + hospitalID
        response = client.query(query)
        print(response)
        available_beds = response[0].Available_beds

        query = "SELECT Zip FROM HOSPITAL WHERE ID = " + hospitalID
        response = client.query(query)
        zip = response[0].Zip

    else:

        beds = "hospital doesn't exist"
        available_beds = "hospital doesn't exist"
        zip = "hospital doesn't exist"

    returnHospital = {

        "total_beds": beds,
        "available_beds": available_beds,
        "zipcode": zip
    }

    return jsonify(returnHospital)



# ---------------------- end of APIs ----------------------

# command to run remotely
app.run('0.0.0.0')

# command to run locally
#app.run()
