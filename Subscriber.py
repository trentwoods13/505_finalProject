#!/usr/bin/env python
import pika
import sys
import json
from DBTools import addNode
from DBTools import addEdge
from DBTools import resetDB


#Make sure orientDB version 2.2 is running, version 3.x does not work with these drivers

#-d run in background login:root password:rootpwd
#docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:2.2

#-it run in foreground login:root password:rootpwd
#docker run -it --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:2.2

resetDB()


# Set the connection parameters to connect to rabbit-server1 on port 5672
# on the / virtual host using the username "guest" and password "guest"



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

print(' [*] Waiting for logs. To exit press CTRL+C')

lastNodeFrom = -1

def callback(ch, method, properties, body):
    print(" [x] %r:%r" % (method.routing_key, body))
    bodystr = body.decode('utf-8')
    data = json.loads(bodystr)
    global lastNodeFrom
    lastNode = -2
    count = 0
    for payload in data:
        mrn = payload['mrn']
        print(mrn)
        addNode(mrn)
        if(lastNodeFrom == -1):
            lastNodeFrom = mrn
        else:
            addEdge(lastNodeFrom,mrn)
            lastNodeFrom = mrn


channel.basic_consume(
    queue=queue_name, on_message_callback=callback, auto_ack=True)

channel.start_consuming()

