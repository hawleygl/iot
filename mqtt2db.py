#!/user/bin/python3

import paho.mqtt.client as mqtt
import psycopg2, sys, getopt

def main(argv):
# str dbhost, dbname, dbuser, dbpass, mqtthost, mqttuser, mqttpass

  try:
    opts, args = getopt.getopt(argv, "h", ["dbhost=", "dbname=", "dbuser=", "dbpass=", "mqtthost=", "mqttuser=", "mqttpass="])
  except getopt.GetoptError:
    print('mqtt2db --dbhost=<db hostname> --dbname=<dbname> --dbuser=<db username> --dbpass=<db password> --mqtthost=<broker hostname> --mqttuser=<broker username> --mqttpass=<mqtt password>')
    sys.exit(2)
  for opt,arg in opts:
    if opt in ("--dbhost"):
      dbhost=arg
    elif opt in ("--dbname"):
      dbname=arg
    elif opt in ("--dbuser"):
      dbuser=arg
    elif opt in ("--dbpass"):
      dbpass=arg
    elif opt in ("--mqtthost"):
      mqtthost=arg
    elif opt in ("--mqttuser"):
      mqttuser=arg
    elif opt in ("--mqttpass"):
      mqttpass=arg
  if((dbhost is None) or (dbname is None) or (dbuser is None) or (dbpass is None) or (mqtthost is None) or (mqttuser is None) or (mqttpass is None)):
    print ('mqtt2db --dbhost=<db hostname> --dbname=<dbname> --dbuser=<db username> --dbpass=<db password> --mqtthost=<broker hostname> --mqttuser=<broker username> --mqttpass=<mqtt password>')
    sys.exit(2)
  print('dbhost:{0} dbname:{1} dbuser:{2} dbpass:{3} mqtthost:{4} mqttuser:{5} mqttpass:{6}'.format(dbhost, dbname, dbuser, dbpass, mqtthost, mqttuser, mqttpass))

  # Database table messages contains receivedAt, topic, message
  # UPDATE THIS WITH CREATE TABLE SQL STATEMENT
  #
  conn = psycopg2.connect("dbname='{0}' user='{1}' host='{2}' password='{3}'".format(dbname, dbuser, dbhost, dbpass))
  cur = conn.cursor()
  conn.autocommit=True

  mqttclient=mqtt.Client()
  mqttclient.username_pw_set(mqttuser, mqttpass)
  mqttclient.on_connect = on_connect
  mqttclient.on_message = on_message

  mqttclient.connect("{0}".format(mqtthost), 1883, 60)

  # Blocking call that processes network traffic, dispatches callbacks and
  # handles reconnecting.
  # Other loop*() functions are available that give a threaded interface and a
  # manual interface.
  mqttclient.loop_forever()



def on_connect(mqttclient, userdata, rc):
  print("Connected with result code "+str(rc))
  # Subscribing in on_connect() means that if we lose the connection and
  # reconnect then subscriptions will be renewed.
  mqttclient.subscribe("#")

# The callback for when a PUBLISH message is received from the server.
def on_message(mqttclient, userdata, msg):
  print(msg.topic+" "+str(msg.payload.decode("utf-8")))
  # this is where we push to the database
  try:
    cur.execute("INSERT INTO messages(topic, message) VALUES (%s,%s)", (msg.topic, str(msg.payload.decode("utf-8"))))
  except ValueError:
    print("Oops! The database didn't like that insert")


if __name__ == "__main__":
  main(sys.argv[1:])
