import flask
from flask import jsonify
# import Subscriber

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




#@app.route('api/reset', methods=['GET'])
#def api_reset():
#    Subscriber.main()
#    reset_status_code = status
#    return jsonify(reset_status_code)



# ---------------------- end of APIs ----------------------

# command to run remotely
app.run('0.0.0.0')

# command to run locally
# app.run()
