import flask
from flask import jsonify

app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/', methods=['GET'])
def home():
    return "<h1>CS505 Final Project</h1>"

names = [
    {
    "team_name": "Team Flatten the Curve",
    "team_member_sids": ["10985181, 12182573"]
    }
]

@app.route('/api/getteam', methods=['GET'])
def returnNames():
    return jsonify(names)

# command to run remotely 
app.run('0.0.0.0')

# command to run locally
# app.run()
