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
    if(index != 'test_data'):
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

    for parent_id in parents_id:
        #the parent msg's author_pub_key will be retrieved, since if a msg is cited => its author is already in the DB 
        addRowToDB(message_id, author_pub_key_string, parent_id)
    print('###\n')


    #add the new msg_id to the MSGDB
    MSGDB.append(message_id)
    writeNewMsg(message_id)
    
    #remove the item in the Queue => allow the loader to upload the next msg
    item = Queue.get()

    print("\nPlease enter a command:\n")


def loadArxivDataset(client):
    #load the dataset used to upload messages

    global Queue

    global DB_articles_authors

    paperId_and_info_and_date_and_keys = pd.read_csv('./Data_to_load/paperId_and_info_and_date_and_keys.csv')

    citations_with_data= pd.read_csv('./Data_to_load/citations(hep-th)_with_Data.csv')

    TOPOLOGICAL_SORT_df= pd.read_csv('./Data_to_load/TOPOLOGICAL_SORT_df.csv')
    TOPOLOGICAL_SORT_df.sort_values(by='0',inplace = True)
    TOPOLOGICAL_SORT_df.reset_index(drop=True,inplace = True)

    #temp df per associare i NodeId dati dal dataset ai msgId dati dalla tangle
    #utile per tracciare articoli caricati e fare referenze a parents (citazioni)
    caricati = pd.DataFrame(columns=['NodeId','msgIdTangle']) 


    #carico messaggio iniziale a cui i nodi di frontiera faranno riferimento
    genesis_msg = client.message(index ='GENESIS-ALLEN',data_str='GENESIS ARTICLE')
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
            message = client.message(index='test_data', data_str=str(message_data), parents = [str(genesis_msg_id)])

        else:
            #per ogni parent NodeId, ottengo il relativo msg_id sulla tangle
            parents_node_id = citations_with_data[citations_with_data['FromNodeId'] == article_id]['ToNodeId']
            parents_msg_id = caricati[caricati['NodeId'].isin(parents_node_id.values)]['msgIdTangle']
        
            parents_msg_id = list(parents_msg_id.values)


            #in base al vincolo della tangle, limito i parents ad 8
            if(len(parents_msg_id)>= 9):
                parents_msg_id = parents_msg_id[0:8]
            
            message = client.message(index='test_data', data_str=str(message_data), parents = parents_msg_id)

        #aggiungo il messaggio caricato 
        caricati.loc[i,'NodeId'] = article_id
        caricati.loc[i,'msgIdTangle'] = str(message['message_id'])

        #inserisco item e mi metto in attesa che MQTT finisca il processing e liberi la coda
        Queue.put('wait')

        #wait per sync (forse non serve)
        #time.sleep(1)
        #print(f'END ITERATION')

    #salva il DB
    DB_articles_authors.to_csv('./DB_articles_authors_after_load.csv',index = False)


def addRowToDB(FromNodeId, From_author_pub_key, ToNodeId):
    #'FromNodeId', 'From_Author_Pub_Key','ToNodeId', 'To_Author_Pub_key'
    #HERE THE 'ToNodeId' VALUE IS THE ID OF THE PARENT MESSAGE CITED BY THE NEW PUB MSG

    global DB_articles_authors

    #each row will be added dinamically to the DB, which init is empty
    # so the row in which will be added is the actual length value
    index_to_add_row = len(DB_articles_authors)
    
    #add the given info
    DB_articles_authors.loc[index_to_add_row,'FromNodeId'] = FromNodeId #aggiungo valori
    DB_articles_authors.loc[index_to_add_row,'From_Author_Pub_Key'] = From_author_pub_key
    DB_articles_authors.loc[index_to_add_row,'ToNodeId'] = ToNodeId


    #retrieve the parent msg Author_Pub_Key cited by the new msg:

    #search in the DB, the message used as parent msg, then get its Author_Pub_Key
    query = DB_articles_authors[DB_articles_authors['FromNodeId'] == ToNodeId]['From_Author_Pub_Key']
    
    
    #query will contain a row for each citation done by this parent msg (ToNodeId)
    if(len(query)>0):
        To_Author_Pub_key = np.unique(query.values)[0]
    else:
        #Not available ogni volta che il messaggio usato come parent è il GENESIS_MSG
        #perche query = autore che ha pubblicato articolo e quindi è in FromNodeId,
        #ma GENESIS MSG non è inserito come FromNodeId, quindi non viene trovato
        To_Author_Pub_key = 'Not_available'

    #add the Author_Pub_Key to the row
    DB_articles_authors.loc[index_to_add_row,'To_Author_Pub_key'] = To_Author_Pub_key


#PR computed on the messages 
def computePageRank():

    print('Strating PageRank computation...\n')


    global DB_articles_authors
    # DB_articles_authors ha le seguenti colonne:
    #'FromNodeId', 'From_Author_Pub_Key','ToNodeId', 'To_Author_Pub_key'

    if(len(DB_articles_authors) == 0):
        print('No messages available to PR computation')
        print('Trying to build the DB of articles citation from the DB authors')
        buildDBArticlesAuthors()

        if(len(DB_articles_authors)==0):
            print('No messages available to PR computation')
            return
        else:
            print(f'#{len(DB_articles_authors)} Messages found, starting PR computation...\n') 

    DB_starting = DB_articles_authors #make a copy (snapshot)

    #estraggo tutti i pub_key introdotti nella piattaforma
    #saranno tutti in From perche:
        #se citi genesis => sei in from, il to è genesis tx
        #se citi articoli => sei in from, il to è l'articolo citato
    From_pub_keys = DB_articles_authors.From_Author_Pub_Key.values


    #NB: prendo solo le pub_keys degli autori in From, perche per costruire il grafo
    # aggiungo archi (e non nodi). Quindi aggiungere tutti gli archi =>
    # aggiungere tutti i nodi contenuti nel grafo

    
    #rimuovo i duplicati e lascio solo pub_keys distinti
    unique_pub_keys = np.unique(From_pub_keys)

    print(f'#UNIQUE PUB KEYS: {len(unique_pub_keys)}')

    #creo grafo vuoto
    D=nx.DiGraph() 

    for i in unique_pub_keys:
        D.add_node(str(i),nodeId = str(i))
    

    #per ogni autore citante
    for citing_pub_key_author in unique_pub_keys:  
        
        #raccolgo sue citazioni nel DB
        outgoing_citation_from_pub_key = DB_starting[DB_starting['From_Author_Pub_Key']==citing_pub_key_author]

        #non considero le citazioni fatte dalla frontiera verso la genesis tx
        outgoing_citation_from_pub_key = outgoing_citation_from_pub_key[outgoing_citation_from_pub_key['To_Author_Pub_key']!= 'Not_available']


        #print('OCFS')
        #print(outgoing_citation_from_pub_key)


        if(len(outgoing_citation_from_pub_key)>0):

            #invidiuo il num di cit fatte verso ogni altro autore
            outgoing_citation_from_pub_key = np.unique(outgoing_citation_from_pub_key.To_Author_Pub_key.values,return_counts=True)

            #print('OCFS2')
            #print(outgoing_citation_from_pub_key)

            #inserisco nel grafo l'arco (autore_citante, autore_citato, num citazioni)
            for i in range(len(outgoing_citation_from_pub_key[0])):

                cited_pub_key_author = outgoing_citation_from_pub_key[0][i] #[0] contiene i diversi valori
                
                num_citations = outgoing_citation_from_pub_key[1][i] #[1] le occorrenze di tali valori
                
                #non considero le auto citazioni per il calcolo del page rank
                if(citing_pub_key_author!=cited_pub_key_author):
                    D.add_weighted_edges_from([(str(citing_pub_key_author),str(cited_pub_key_author),num_citations)])

    nx.write_weighted_edgelist(D, 'graph.csv')



    #ottengo matrice adiacenza da grafo
    #AM_sparse =nx.adjacency_matrix(D) #list storage type
    AM_matrix = nx.adjacency_matrix(D, nodelist=D.nodes()).todense()

    AM_matrix_df = pd.DataFrame(AM_matrix)
    AM_matrix_df.set_index(unique_pub_keys,inplace = True)
    AM_matrix_df.columns = unique_pub_keys

    print(f'ADJM len: {len(AM_matrix)}')
    #print(f'Adjacency Matrix on authors citation: {AM_matrix_df}')
    AM_matrix_df.to_csv('./AM_Matrix.csv')
    #np.savetxt(r'./AM_Matrix.txt', AM_matrix, fmt='%d')

    #calcolo PageRank
    PR = nx.pagerank(D)
    PR_df = pd.DataFrame.from_dict(PR,orient='index') 
    print(f'Page Rank Values: {PR_df}')
    PR_df.to_csv('./PR_df.csv')  

    print('PR computed') 


#if client has restarted, the DB_articles_authors is empty, we can reconstruct it
#by reading the MSGDB stored and get all msg_s published  
def buildDBArticlesAuthors():

    global DB_articles_authors

    global MSGDB

    print('Building the local DB of message_id...\n')

    for i in range (len(MSGDB)):
        message_id = MSGDB[i]

        #for each msg in the MSGDB retrieve its info
        message = client.get_message_data(message_id)

        parents = message['parents']

        #print("MESSAGE:")
        #print(message)

        #non mi serve qua
        index = message['payload']['indexation'][0]['index']
        index = (bytes.fromhex(index).decode('utf-8'))
    
        #print(index)

        data = message['payload']['indexation'][0]['data']
        data = str(bytearray(data).decode('utf-8'))
        #print(data)

        data_splitted = data.split('#')

        author_pub_key_string = data_splitted[1]

        #signature_string = data_splitted[2]

        #text_data_string = data_splitted[3]




        for parent_id in parents:
            #the parent msg's author_pub_key will be retrieved, since if a msg is cited => its author is already in the DB 
            addRowToDB(message_id, author_pub_key_string, parent_id)
        #print('#######')

    DB_articles_authors.to_csv('./DB_articles_authors_built.csv',index = False)

    print('DB_articles_authors reconstructed\n')

    return DB_articles_authors


#write a new msg id to the DB file
def writeNewMsg(msg_id):
    
    file = open("lista_msgDB.txt", "a")
    file.write(msg_id+'\n')        
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
    except:
        
        msg_DB = []

    return msg_DB


#VARIABLES DEFINED HERE ARE VISIBILE EVERYWHERE.
#IF I WANT TO MODIFY/USE THEM INSIDE A FUN, I'VE TO REFER TO THEM THROUGH "GLOBAL"
#KEYWORD INSIDE THAT FUN, OR IT WILL CREATE ANOTHER LOCAL VAR


#queue used to synch the messages loader and the MQTT messages receiver 
Queue = queue.Queue(maxsize=1)

# create a client with a node
client = iota_client.Client(
    nodes_name_password=[['http://0.0.0.0:14265']])


MSGDB = readMsgDB()

DB_articles_authors = pd.DataFrame(columns=['FromNodeId', 'From_Author_Pub_Key','ToNodeId', 'To_Author_Pub_key'])




def main():

    print("\n##########")
    print('Starting init operation for setup...\n')

    #se ci sono messaggi memorizzati in locale, ricostruisco subito il DB locale di msgs
    if(len(MSGDB)>0):
        DB_articles_authors = buildDBArticlesAuthors()
        

    #subscribe to MQTT topic    
    client.subscribe_topic('messages',MQTT_callback)
    print('MQTT Subscription done\n')

    user_command = ''

    while(user_command!='exit'):
        user_command = input("\nPlease enter a command:\n")
        
        if(user_command == 'print_DB'):
            print(f'Len DB_articles_authors: {len(DB_articles_authors)}')

        if(user_command == 'PR'):
            computePageRank()

        if(user_command == 'LAD'):
            loadArxivDataset(client)
        


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