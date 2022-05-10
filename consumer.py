#!/usr/bin/env python
#
# Copyright 2020 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# =============================================================================
#
# Consume messages from Confluent Cloud
# Using Confluent Python Client for Apache Kafka
#
# =============================================================================

from confluent_kafka import Consumer
import json
import ccloud_lib
import psycopg2
import datetime
import time

DBname = "postgres"
DBuser = "postgres"
DBpwd = "0000"

def dbconnect():
    connection = psycopg2.connect(
            host="localhost",
            database=DBname,
            user=DBuser,
            password=DBpwd,
            )
    connection.autocommit = False
    return connection

def write_json(new_data, filename='data.json'):
    with open(filename,'r+') as file:
          # First we load existing data into a dict.
        file_data = json.load(file)
        # Join new_data with file_data inside emp_details
        file_data["count"].append(new_data)
        # Sets file's current position at offset.
        file.seek(0)
        # convert back to json.
        json.dump(file_data, file, indent = 4)

if __name__ == '__main__':

    # Read arguments and configurations and initialize
    args = ccloud_lib.parse_args()
    config_file = args.config_file
    topic = args.topic
    conf = ccloud_lib.read_ccloud_config(config_file)

    # Create Consumer instance
    # 'auto.offset.reset=earliest' to start reading from the beginning of the
    #   topic if no committed offsets exist
    consumer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
    consumer_conf['group.id'] = 'python_example_group_1'
    consumer_conf['auto.offset.reset'] = 'earliest'
    consumer = Consumer(consumer_conf)

    # Subscribe to topic
    consumer.subscribe([topic])

    # Process messages
    total_count = 0
    count2 = 0

    conn = dbconnect()

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # No message available within timeout.
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                print('error: {}'.format(msg.error()))
            else:
                # Check for Kafka message
                record_key = msg.key()
                record_value = msg.value()
                data = json.loads(record_value)
                #write_json(data)
                #count = data['count']
                total_count += count2
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(record_key, record_value, total_count))
                #THIS IS WHERE WE WILL COPY AND INSERT THE DATA INTO THE DATABASE!!!!!!
                #Connect to Database
                date = data['count']['OPD_DATE']
                tstamp = date + " " + datetime.datetime.fromtimestamp(int(data['count']['ACT_TIME'])).strftime('%H:%M:%S')
                
               # tstamp = r"'2016-06-22 19:10:25-07'"
                #tstamp = "'" + tstamp1 + "'"
                print("This is tstamp",tstamp)


                if(data['count']['GPS_LATITUDE'] == ''):
                    latitude = None 
                else:
                    latitude = data['count']['GPS_LATITUDE']

                if(data['count']['GPS_LONGITUDE'] == ''):
                    longitude = None
                else:
                    longitude = data['count']['GPS_LONGITUDE'] 
                    
                if( data['count']['DIRECTION'] == ''):
                    direction = None
                else:
                    direction = data['count']['DIRECTION']

                if(data['count']['VELOCITY'] == ''):
                    speed = None #Convert from meters per second to mile per hour
                else:
                    speed = 2.2369 * int(data['count']['VELOCITY'])

                if(data['count']['DIRECTION'] == ''):
                    direction = None #Convert from meters per second to mile per hour
                else:
                    direction = data['count']['DIRECTION'] 

                if(data['count']['EVENT_NO_TRIP'] == ''):
                    trip_id = None
                else:
                    trip_id = data['count']['EVENT_NO_TRIP']
            
                route_id = None

                if(data['count']['VEHICLE_ID'] == ''):
                    vehicle_id = None
                else:
                    vehicle_id = data['count']['VEHICLE_ID']

                service_key = None
                direction_trip_table = None

                check = "SELECT count(*) FROM trip WHERE trip_id = " + trip_id

                #conn = dbconnect()
                with conn.cursor() as cursor:

                    #cursor.execute(check)
                    #data=cursor.fetchone()[0]
                    #if data == 0:
                    cursor.execute("INSERT INTO trip VALUES (%s,%s,%s,%s,%s)", (trip_id,route_id,vehicle_id,service_key,direction_trip_table))
                    cursor.execute("INSERT INTO breadcrumb VALUES (%s,%s,%s,%s,%s,%s)", (tstamp,latitude,longitude,direction,speed,trip_id))
                    conn.commit()
                   # else:
#                        print('Data exists going to next')

                #load(conn, breadcrumbQuery, tripTableQuery)


    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
