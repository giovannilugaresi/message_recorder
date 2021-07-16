# -*- coding: utf-8 -*-
"""
Created on Fri Jul 16 11:48:26 2021

@author: giova
"""

import paho.mqtt.client as mqtt
import json
import pickle
import time
from time import sleep



msg_list = []


# %% USER INPUT

client_data = {'address': "localhost", 'port': 1883, 'timeout' : 60}

topics = ["topic/activity", "topic/states", "topic/buffers", "topic/operators"]

time_to_record = 30 #how long (seconds) you wish to record


# %% FUNCTIONS

def on_connect(client, userdata, flags, rc):
    
    print("Connected with result code "+str(rc)+ "\n")
#    client.subscribe("topic/activity")
#    client.subscribe("topic/states")
#    client.subscribe("topic/buffers")
#    client.subscribe("topic/operators")
  

def on_message(client, userdata, msg):
    

    print(msg.payload.decode())
    
#    global msg_list
    msg_list.append(    {'topic': msg.topic , 'payload': json.loads(msg.payload.decode()), 'ts': time.time()})
#
    

def record(client, time = 30, topics = [], client_data ={'address': "localhost", 'port': 1883, 'timeout' : 60}):
    
    """
    # funzione che fa partire il loop, la registrazione è in on_message    
    
    client: MQTT client object
     
    time: time of recording in seconds (default is 30 s)
    
    topics: list of topics to be recorded
    
    client data: dictionary to connect , example is also default > client_data = {'address': "localhost", 'port': 1883, 'timeout' : 60}

    """
    
    client.connect(client_data['address'],client_data['port'],client_data['timeout'])
    
    for tp in topics:
        client.subscribe(tp)
    
    print("starting recording...")
    
    client.loop_start()

    i = 0
    while i < time:
        sleep(1)
        i= i + 1
        
    client.loop_stop()
    client.disconnect()
    
    print("recording stopped")
    
    
    
def stream(client, msg_list, client_data={'address': "localhost", 'port': 1883, 'timeout' : 60}):

    """
    # funzione che streamma indietro i messaggi mqtt su stessi topic, con stessi intervalli temporali
    
    client: MQTT client object
    
    msg_list: list of dictionaries identifying the messages arrived and recorded, example > {'topic': 'activity' , 'payload': {'activity': 2, 'id': 1234, 'ts': 1}, 'ts': 1334212}
    
    client data: dictionary to connect , example is also default > client_data = {'address': "localhost", 'port': 1883, 'timeout' : 60}

    """
    # extract minimum timestamp
    tmin = time.time()
    
    for msg in msg_list:
        
        if msg['ts'] < tmin:
            tmin = msg['ts']
            
    
    client.connect(client_data['address'],client_data['port'],client_data['timeout'])

    client.loop_start()
    
    print("\n starting to stream the messages... \n")
    
    for msg in msg_list:
        
        sleep( msg['ts'] - tmin)
        client.publish(msg['topic'], str(msg['payload']))
    
    print("all messages have been streamed, stopping the client... \n")

    client.loop_stop()
    client.disconnect()
        
    
# %% START CLIENT
    
client = mqtt.Client()

client.on_connect = on_connect
client.on_message = on_message
#client.record = record


# %% RECORDING

#client.loop_start()
#
#condition = True
#i = 0
#while i < 30:
#    sleep(1)
#    i= i + 1
#    
#client.loop_stop()
#client.disconnect()


record(client, time_to_record, topics, client_data)

with open('msg_list.pickle', 'wb') as handle:
    pickle.dump(msg_list, handle, protocol=pickle.HIGHEST_PROTOCOL)



# %% STREAMING
    
with open('msg_list.pickle', 'rb') as handle:
    msg_list = pickle.load(handle)

stream(client, msg_list, client_data)