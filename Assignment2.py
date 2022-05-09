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

def load(conn, breadcrumbQuery, tripTableQuery):
    
    with conn.cursor() as cursor:
        cursor.execute(tripTableQuery)
        cursor.execute(breadcrumbQuery)


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
                #count = data['count']
                total_count += count2
                print("Consumed record with key {} and value {}, \
                      and updated total count to {}"
                      .format(record_key, record_value, total_count))
                #THIS IS WHERE WE WILL COPY AND INSERT THE DATA INTO THE DATABASE!!!!!!
                #Connect to Database
                tstamp1 = datetime.datetime.fromtimestamp(int(data['count']['ACT_TIME'])).strftime('%H:%M:%S')
               # tstamp = r"'2016-06-22 19:10:25-07'"
                tstamp = "'" + tstamp1 + "'"
                print("This is tstamp",tstamp)


                latitude = data['count']['GPS_LATITUDE']
                longitude = data['count']['GPS_LONGITUDE']
                direction = data['count']['DIRECTION']
                if(data['count']['VELOCITY'] == ''):
                    speed = 'NULL' #Convert from meters per second to mile per hour
                else:
                    speed = 2.2369 * data['count']['VELOCITY']  
                if(data['count']['DIRECTION'] == ''):
                    direction = 'NULL' #Convert from meters per second to mile per hour
                else:
                    direction = data['count']['DIRECTION'] 

                trip_id = data['count']['EVENT_NO_TRIP']
                #route_id = ''
                vehicle_id = data['count']['VEHICLE_ID']
                #service_key = ''
                #direction_trip_table = ''
                #valstr1 = tstamp + "," + latitude + "," + longitude + "," + direction + "," + speed + "," + trip_id
                print("This is speed", speed)
                print("This is direction", direction)
                #valstr2 = trip_id + "," + 'NULL' + "," + vehicle_id + "," + 'NULL' + "," + 'NULL' 
                #breadcrumbQuery = f"INSERT INTO breadcrumb VALUES ({valstr1});"
                #tripTableQuery = f"INSERT INTO trip VALUES ({valstr1});"
                conn = dbconnect()
                with conn.cursor() as cursor:
                    cursor.execute("INSERT INTO trip (trip_id,route_id,vehicle_id,service_key,direction) VALUES (trip_id,NULL,vehicle_id,NULL,NULL)")
                    cursor.execute("INSERT INTO breadcrumb (tstamp,latitude,longitude,direction,speed,trip_id) VALUES (tstamp,latitude,longitude,direction,speed,trip_id)")



                #load(conn, breadcrumbQuery, tripTableQuery)


    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
