import pyorient
import csv
import re

def reset_db(client, name):

   # Remove Old Database
   if client.db_exists(name):
    client.db_drop(name)

   # Create New Database
   client.db_create(name,
      pyorient.DB_TYPE_GRAPH,
      pyorient.STORAGE_TYPE_PLOCAL)

def importHospitals(client):
    print("Importing hospitals into database...")
    #open csv file of hospitals
    filepath = "hospitals.csv"
    file =  open(filepath)
    readFile = csv.reader(file, delimiter = ',')
    headers = []
    rowCount = 0
    for row in readFile:
        hospitalInfo = []
        for i in range(len(row)):
            #gets rid of hidden garbage characters and single quotes
            row[i] = re.sub("'", "", row[i])
            row[i] = re.sub("[^\x20-\x7E]", "", row[i])
            if rowCount == 0:
                #get data headers
                headers.append(row[i])
            else:
                #get hospital info
                hospitalInfo.append(row[i])
        if rowCount != 0:
            #build query to insert hospital into database
            query = "CREATE VERTEX Hospital SET "
            for i in range(len(hospitalInfo)):
                query += headers[i] + " = " +  "'" + hospitalInfo[i] + "'"
                if (i != len(hospitalInfo) - 1):
                    query += ","
            #insert hospital into database
            client.command(query)
        rowCount += 1
    
#NEED TO FINISH---------
#need to make it so that whenever a patient is inserted, we check if they need to be routed to a hospital
#if so, connect them to that hospital in the database and reduce the bed count by 1
def insertPatient(first_name, last_name, mrn, zip_code, patient_status_code, client):
    query = "CREATE VERTEX Patient SET first_name = '" + first_name + "', last_name = '" + last_name + "', mrn = '" + mrn + "', zip_code = '" + zip_code + "', patient_status_code = '" + patient_status_code + "'"
    client.command(query)
    #client.close()
    

def loadDB():
    print("Starting database...")
    #database name
    dbname = "covid_reporting_project"
    #database login is root by default
    login = "root"
    #database password, set by docker param
    password = "rootpwd"

    #create client to connect to local orientdb docker container
    client = pyorient.OrientDB("172.31.144.183", 2424)
    try:
        session_id = client.connect(login, password)
    except:
        return False

    #remove old database and create new one
    reset_db(client,dbname)

    #open the database we are interested in
    client.db_open(dbname, login, password)

    client.command("CREATE CLASS Patient EXTENDS V")
    client.command("CREATE PROPERTY Patient.first_name String")
    client.command("CREATE PROPERTY Patient.last_name String")
    client.command("CREATE PROPERTY Patient.mrn String")
    client.command("CREATE PROPERTY Patient.zip_code Integer")
    client.command("CREATE PROPERTY Patient.patient_status_code Integer")
    
    client.command("CREATE CLASS Hospital EXTENDS V")
    client.command("CREATE PROPERTY Hospital.ID Integer")
    client.command("CREATE PROPERTY Hospital.Name String")
    client.command("CREATE PROPERTY Hospital.Address String")
    client.command("CREATE PROPERTY Hospital.City String")
    client.command("CREATE PROPERTY Hospital.State String")
    client.command("CREATE PROPERTY Hospital.Zip Integer")
    client.command("CREATE PROPERTY Hospital.Type String")
    client.command("CREATE PROPERTY Hospital.Beds Integer")
    client.command("CREATE PROPERTY Hospital.County String")
    client.command("CREATE PROPERTY Hospital.Countyfips Integer")
    client.command("CREATE PROPERTY Hospital.Country String")
    client.command("CREATE PROPERTY Hospital.Latitude Float")
    client.command("CREATE PROPERTY Hospital.Longitude Float")
    client.command("CREATE PROPERTY Hospital.Naics_code Integer")
    client.command("CREATE PROPERTY Hospital.Website String")
    client.command("CREATE PROPERTY Hospital.Owner String")
    client.command("CREATE PROPERTY Hospital.Trauma String")
    client.command("CREATE PROPERTY Hospital.Helipad String")
    
    importHospitals(client)
    
    client.close()
    return True