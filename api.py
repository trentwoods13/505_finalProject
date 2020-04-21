import flask
from flask import jsonify
import DBTools
import pyorient

app = flask.Flask(__name__)
app.config["DEBUG"] = True


status = 1
names = [
    {
    "team_names": "Team Flatten the Curve",
    "team_member_sids": ["10985181, 12182573"],
    "status": status
    }
]



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
    client = pyorient.OrientDB("172.31.147.250", 2424)
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


#@app.route('api/reset', methods=['GET'])
#def api_reset():
#    Subscriber.main()
#    reset_status_code = status
#    return jsonify(reset_status_code)



# ---------------------- end of APIs ----------------------

# command to run remotely
#app.run('0.0.0.0')

# command to run locally
app.run()
