''' 
-Jani Leinonen 29.3.2019-
Tested with Python 3.6.5

The purpose of this program:

1. Send data to kafka topic.
2. Get the previously sent data from kafka topic.
3. Write the data to database and read the data from database to check that all functioned.

Kafka topic and the database are manually created from the portal and they need to be running before running this software.

'''

from kafka import KafkaProducer, KafkaConsumer
import threading, psycopg2, time

# Dictionary for the messages collect from the kafka topic.
messages = {}

# Method for polling the data from kafka topic. When data is found it's written to messages dictionary.
def runConsumer():
	consumer = KafkaConsumer(
		"demo-topic",
		bootstrap_servers="kafka-3cf79b5a-jani-cb65.aivencloud.com:17545",
		client_id="demo-client-1",
		group_id="demo-group",
		security_protocol="SSL",
		ssl_cafile="ca.pem",
		ssl_certfile="service.cert",
		ssl_keyfile="service.key"
		)
	print('Consumer started')
	
	for index, item in enumerate(consumer):
		time.sleep(0.1)
		messages[str(index + 1)] = item.value
		if(len(messages) == 3):
			print('Messages acquired')
			break

# Start consumer within it's own thread so it can listen the topic.
threading.Thread(target=runConsumer).start()

# Initialize producer
producer = KafkaProducer(
	bootstrap_servers = "kafka-3cf79b5a-jani-cb65.aivencloud.com:17545",
	security_protocol = "SSL",
	ssl_cafile = "ca.pem",
	ssl_certfile = "service.cert",
	ssl_keyfile = "service.key"
	)
print('Producer started')

# Use producer to write some data to the topic
loop_counter = 0
while(len(messages) < 3):
	for i in range(1, 4):
		message = "TestMessageNumber{}".format(i)
		producer.send("demo-topic", message.encode("utf-8"))
		producer.flush()
	time.sleep(3)
	loop_counter += 1
	if(loop_counter > 10):
		raise Exception("It's taking too long to complete")

# Write the data to the database
conn = psycopg2.connect(database="defaultdb", user="avnadmin", password="cvyhxd46241pnjc1", host="pg-2562b3d8-jani-cb65.aivencloud.com", port="17543")
print('Connected to database')
cur = conn.cursor()
cur.execute("CREATE TABLE IF NOT EXISTS Testing(Id INTEGER PRIMARY KEY, Data VARCHAR(100))")
for key, value in messages.items():
	newvalue = value.decode("utf-8")
	dbcommand = "INSERT INTO Testing VALUES(%s, '%s')" %(key, newvalue)
	cur.execute(dbcommand)
conn.commit()

#Check that the data is in database
cur.execute("select * from Testing")
psql_data = cur.fetchall()
print("\nThis is the data:")
print(psql_data)

#Empty the testing table for a potential new test
cur.execute("delete from Testing")
conn.commit()

cur.close()
conn.close()