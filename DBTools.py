import pyorient

def getrid(client,id):
    nodeId = client.query("SELECT FROM V WHERE id = '" + str(id) + "'")
    if(nodeId):
        return str(nodeId[0]._rid)
    else:
        return "0"

def resetDB():

    # database name
    name = "agen"
    # database login is root by default
    login = "root"
    # database password, set by docker param
    password = "rootpwd"

    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    # Remove Old Database
    if client.db_exists(name):
        client.db_drop(name)

    # Create New Database
    client.db_create(name,
      pyorient.DB_TYPE_GRAPH,
      pyorient.STORAGE_TYPE_PLOCAL)


def addNode(nodeId):

    #database name
    dbname = "agen"
    #database login is root by default
    login = "root"
    #database password, set by docker param
    password = "rootpwd"

    #create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    #open the database we are interested in
    client.db_open(dbname, login, password)

    client.command("CREATE VERTEX SET id = '" + nodeId + "'")

    client.close()


def addEdge(nodeFrom, nodeTo):
    # database name
    dbname = "agen"
    # database login is root by default
    login = "root"
    # database password, set by docker param
    password = "rootpwd"

    # create client to connect to local orientdb docker container
    client = pyorient.OrientDB("localhost", 2424)
    session_id = client.connect(login, password)

    # open the database we are interested in
    client.db_open(dbname, login, password)

    nodeFromId = str(getrid(client, nodeFrom))
    nodeToId = str(getrid(client, nodeTo))

    client.command("CREATE EDGE FROM " + nodeFromId + " TO " + nodeToId)

    client.close()
