import os 
import pandas as pd
import binascii
import iota_client
import hashlib
import json
import csv
import numpy as np
import networkx as nx
import queue
import time
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding
from cryptography.hazmat.primitives import serialization
from cryptography.exceptions import InvalidSignature


def MQTT_callback(msg):
    #the msg received is a structured string, it will be converted as dict and 
    #its field will be read 

    global Queue

    global MSGDB
    #print('MSG')
    #print(msg)

    #digging in the msg fields
    dict = json.loads(msg)

    message_id = client.get_message_id(str(dict['payload']))
    #print(message_id)

    dict = dict['payload']
    dict = json.loads(dict)

    parents_id = dict['parents']

    #print(parents_id)

    dict = dict['payload']

    index = str(bytearray(dict['data']['index']).decode('utf-8'))

    #print(index)

    #skip if the genesis msg is evaluated => its info are not recorded in the DBs
    if(index == 'GENESIS-MSG'):
        print('Genesis')
        return
    


    
    data = str(bytearray(dict['data']['data']).decode('utf-8'))
    #print(f'DATA RICEVUTA: {data}')

    data_splitted = data.split('#')

    author_pub_key_string = data_splitted[1]

    signature_string = data_splitted[2]

    text_data_string = data_splitted[3]

    #before processing this msg, check if the signature is correct or not
    
    text_data_bytes = str.encode(text_data_string)

    signature_bytes = bytes.fromhex(signature_string)

    author_pub_key_bytes = str.encode(author_pub_key_string)

    #print('mqtt decodificato')

    try:
        serialization.load_pem_public_key(
            author_pub_key_bytes,
        ).verify(
            signature_bytes,
            text_data_bytes,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.MAX_LENGTH
            ),
            hashes.SHA256()
        )

        print('OK SIGNATURE')

    except InvalidSignature:
        print('Signature Verification Failed, the received message will be ignored')
        return

    print('####### \n MQTT msg pub:')
    #print(f'MsgId: {message_id}\n')
    #print(f'Public Key author: {author_pub_key_string}')
    #print(f'Data: {data}\n')
    #print(f'Parents: {parents_id}\n')

    #here, given the published msg we've: msg_id, auth_pub_key, [parents_id]
    #for each citation done by the published msg, we add a row in the DB


    #add the new msg_id to the MSGDB
    MSGDB.append(message_id)
    print(f'LEN MSG_DB: {len(MSGDB)}')
    writeNewMsg(message_id)


    if(index not in TOPICS):
        TOPICS.append(index)
        writeNewTopic(index)
    
    #remove the item in the Queue => allow the loader to upload the next msg
    item = Queue.get()

    print("\nPlease enter a command:\n")


def loadArxivDataset():
    #load the dataset used to upload messages

    global Queue

    global DB_articles_authors

    global client

    paperId_and_info_and_date_and_keys = pd.read_csv('./Data_to_load/paperId_and_info_and_date_and_keys.csv')

    citations_with_data= pd.read_csv('./Data_to_load/citations(hep-th)_with_Data.csv')

    TOPOLOGICAL_SORT_df= pd.read_csv('./Data_to_load/TOPOLOGICAL_SORT_df.csv')
    TOPOLOGICAL_SORT_df.sort_values(by='0',inplace = True)
    TOPOLOGICAL_SORT_df.reset_index(drop=True,inplace = True)

    #temp df per associare i NodeId dati dal dataset ai msgId dati dalla tangle
    #utile per tracciare articoli caricati e fare referenze a parents (citazioni)
    caricati = pd.DataFrame(columns=['NodeId','msgIdTangle']) 


    #carico messaggio iniziale a cui i nodi di frontiera faranno riferimento
    genesis_msg = client.message(index ='GENESIS-MSG',data_str='GENESIS ARTICLE')
    genesis_msg_id = genesis_msg['message_id']


    for i in range(len(TOPOLOGICAL_SORT_df)):

        print(f'I TOPOL SORT: {i}')
        #wait the MQTT reader to finish the processing of the last msg uploaded
        #while(Queue.qsize()>0):
        #    pass
        
        #print('DOPO WHILE')

        #info sull'articolo da caricare
        article_id = TOPOLOGICAL_SORT_df.iloc[i]['0']
        
        #pem = serializzazione della chiave, da oggetto di classe a stringa
        private_pem_string = paperId_and_info_and_date_and_keys[paperId_and_info_and_date_and_keys['NodeId']==article_id]['PrivateKey'].values[0]
        public_pem_string = paperId_and_info_and_date_and_keys[paperId_and_info_and_date_and_keys['NodeId']==article_id]['PublicKey'].values[0]

        #print('PR & PUB')
        #print(private_pem+'\n')
        #print(public_pem+'\n')

        private_pem_bytes = str.encode(private_pem_string)
        #public_pem_bytes = str.encode(public_pem_string)

        title = paperId_and_info_and_date_and_keys[paperId_and_info_and_date_and_keys['NodeId']==article_id]['Title']
        date = paperId_and_info_and_date_and_keys[paperId_and_info_and_date_and_keys['NodeId']==article_id]['Date']
    
        message_txt_data_string = str(title.values[0]+'\n Date: '+date.values[0])
        message_txt_data_bytes = str.encode(message_txt_data_string)

        #print(f'DECODIFICATO')

        signature = serialization.load_pem_private_key(
            private_pem_bytes,
            password=None,
            ).sign(
                message_txt_data_bytes,
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH
                ),
                hashes.SHA256()
            )
        #print('FIRMA FATTA')

        signature_string = signature.hex()

        #the '#' symbol is our separator

        message_data = '#'+public_pem_string+'#'+signature_string+'#'+message_txt_data_string+'#'


        #print('MESSAGE BUILDATO')

        #se sei un nodo di frontiera 
        if(article_id not in citations_with_data['FromNodeId'].values):
            message = client.message(index='#test_data', data_str=str(message_data), parents = [str(genesis_msg_id)])

        else:
            #per ogni parent NodeId, ottengo il relativo msg_id sulla tangle
            parents_node_id = citations_with_data[citations_with_data['FromNodeId'] == article_id]['ToNodeId']
            parents_msg_id = caricati[caricati['NodeId'].isin(parents_node_id.values)]['msgIdTangle']
        
            parents_msg_id = list(parents_msg_id.values)


            #in base al vincolo della tangle, limito i parents ad 8
            if(len(parents_msg_id)>= 9):
                parents_msg_id = parents_msg_id[0:8]
            
            message = client.message(index='#test_data', data_str=str(message_data), parents = parents_msg_id)

        #aggiungo il messaggio caricato 
        caricati.loc[i,'NodeId'] = article_id
        caricati.loc[i,'msgIdTangle'] = str(message['message_id'])

        #inserisco item e mi metto in attesa che MQTT finisca il processing e liberi la coda
        Queue.put('wait')

        #wait per sync (forse non serve)
        #time.sleep(1)
        #print(f'END ITERATION')

    #salva il DB
    

#write a new msg id to the DB file
def writeNewMsg(msg_id):
    
    file = open("lista_msgDB.txt", "a")
    file.write(msg_id+'\n')        
    file.close() 



def writeNewTopic(index_topic):
    file = open("lista_topic.txt", "a")
    file.write(index_topic+'\n')        
    file.close()






#load the msg_id list from DB file to a Global variable
def readMsgDB():
    print('Checking for local message_id stored...\n')
    try: 
        file = open("lista_msgDB.txt", "r")
        msg_DB = file.readlines()

        for i in range(len(msg_DB)):
            msg_DB[i] = msg_DB[i].replace("\n", "")

        print(f'Found #{len(msg_DB)} messages from the file lista_msgDB.txt \n')

        file.close() 
    except:
        
        msg_DB = []

    
    return msg_DB




#load the msg_id list from DB file to a Global variable
def readTopics():
    print('Checking for local topics stored...\n')
    try: 
        file = open("lista_topic.txt", "r")
        indices_topics = file.readlines()

        for i in range(len(indices_topics)):
            indices_topics[i] = indices_topics[i].replace("\n", "")

        print(f'Found #{len(indices_topics)} messages from the file lista_msgDB.txt \n')

        file.close() 
    except:
        
        indices_topics = []

    
    return indices_topics


#VARIABLES DEFINED HERE ARE VISIBILE EVERYWHERE.
#IF I WANT TO MODIFY/USE THEM INSIDE A FUN, I'VE TO REFER TO THEM THROUGH "GLOBAL"
#KEYWORD INSIDE THAT FUN, OR IT WILL CREATE ANOTHER LOCAL VAR


#queue used to synch the messages loader and the MQTT messages receiver 
Queue = queue.Queue(maxsize=1)

# create a client with a node
client = iota_client.Client(
    nodes_name_password=[['http://0.0.0.0:14265']])


MSGDB = readMsgDB()

TOPICS = readTopics()



def main():

    print("\n##########")
    print('Starting init operation for setup...\n')


    #subscribe to MQTT topic    
    client.subscribe_topic('messages',MQTT_callback)
    print('MQTT Subscription done\n')


    loadArxivDataset()

    user_command = ''

    while(user_command!='exit'):
        user_command = input("\nListening for new incoming messages:\n")

    '''
    user_command = ''

    while(user_command!='exit'):
        user_command = input("\nPlease enter a command:\n")
        
        if(user_command == 'print_DB'):
            print(f'Len DB_articles_authors: {len(DB_articles_authors)}')

        if(user_command == 'PR'):
            computePageRank()

        if(user_command == 'LAD'):
            loadArxivDataset()
    
    '''
        


if __name__ == "__main__":
    main()


'''
def refresh_DBArticlesAuthors():
    global DB_articles_authors

    for i in range(len(DB_articles_authors)):
        if(DB_articles_authors.iloc[i]['To_Author_Seed']=='Not_available'):
            ToNodeId = DB_articles_authors.iloc[i]['ToNodeId']

            if(ToNodeId in DB_articles_authors['FromNodeId'].values):
                q = DB_articles_authors[DB_articles_authors['FromNodeId']== ToNodeId]['From_Author_Seed']
                To_Author_Seed = np.unique(q.values)[0]
                print(To_Author_Seed)
                DB_articles_authors.iloc[i]['To_Author_Seed'] = str(To_Author_Seed)
    
    DB_articles_authors.to_csv('./DB_articles_authors2.csv',index = False)
'''