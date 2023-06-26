import os 
import pandas as pd
import binascii
import iota_client
import hashlib
import json
import csv
 
INDEXAUTHORSLIST = []

def MQTT_callback(msg):
    #the msg received is a structured string, it will be converted as dict and 
    #its field will be read 

    #digging in the msg fields
    dict = json.loads(msg)
    dict = dict['payload']
    dict = json.loads(dict)
    dict = dict['payload']
    
    author_index = str(bytearray(dict['data']['index']).decode('utf-8'))
    data = str(bytearray(dict['data']['data']).decode('utf-8'))

    print('MQTT msg pub:')
    print(f'Index author: {author_index}')
    print(f'Data: {data}\n')
    print('###\n')

    #verify if the author index is new or already in DB
    #if new => add to DB
    if(author_index not in INDEXAUTHORSLIST):
        INDEXAUTHORSLIST.append(author_index)
        writeNewIndexAuthor(author_index)


#write a new Index Author to the DB file
def writeNewIndexAuthor(author_index):
    
    file = open("lista_indici.txt", "a")
    file.write(author_index+'\n')        
    file.close() 



#load the index authors list from DB file to a Global variable
def readIndexAuthorsList():
    try: 
        file = open("lista_indici.txt", "r")
        index_authors_list = file.readlines()

        for i in range(len(index_authors_list)):
            index_authors_list[i] = index_authors_list[i].replace("\n", "")

        print(index_authors_list)
    except:
        
        index_authors_list = []

    return index_authors_list







def main():
    #INIT CONNECTION TO NODE AND SEED/ADDRESS RETRIEVING

    print("\n##########")

    # create a client with a node
    client = iota_client.Client(
        nodes_name_password=[['http://0.0.0.0:14265']])


    INDEXAUTHORSLIST = readIndexAuthorsList()

    #subscribe to MQTT topic    
    client.subscribe_topic('messages',MQTT_callback)
    print('MQTT Subscription')

    user_command = ''

    while(user_command!='exit'):
        user_command = input("Please enter a command:\n")



if __name__ == "__main__":
    main()