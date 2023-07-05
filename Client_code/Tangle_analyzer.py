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

#list/DB of authors that uploaded articles on the platform
global AUTHORSDB

#list/DB of all the message ids uploaded in the platform 
global MSGDB

#on init create the DB that stores articles and citations
global DB_articles_authors
DB_articles_authors = pd.DataFrame(columns=['FromNodeId', 'From_Author_Seed','ToNodeId', 'To_Author_Seed'])

#queue used to synch the messages loader and the MQTT messages receiver 
global Queue
Queue = queue.Queue(maxsize=1)

# create a client with a node
client = iota_client.Client(
        nodes_name_password=[['http://0.0.0.0:14265']])


def MQTT_callback(msg):
    #the msg received is a structured string, it will be converted as dict and 
    #its field will be read 

    global Queue

    #digging in the msg fields
    dict = json.loads(msg)

    message_id = client.get_message_id(str(dict['payload']))

    dict = dict['payload']
    dict = json.loads(dict)

    parents_id = dict['parents']

    dict = dict['payload']
    
    author_seed = str(bytearray(dict['data']['index']).decode('utf-8'))

    #skip if the genesis msg is evaluated => its info are not recorded in the DBs
    if(author_seed == 'GENESIS-ALLEN'):
        return

    data = str(bytearray(dict['data']['data']).decode('utf-8'))

    print('####### \n MQTT msg pub:')
    print(f'MsgId: {message_id}\n')
    print(f'Index author: {author_seed}')
    print(f'Data: {data}\n')
    #print(f'Parents: {parents_id}\n')

    #here, given the published msg we've: msg_id, auth_seed, [parents_id]
    #for each citation done by the published msg, we add a row in the DB

    for parent_id in parents_id:
        #the parent msg's author_seed will be retrieved, since if a msg is cited => its author is already in the DB 
        addRowToDB(message_id, author_seed, parent_id)
    print('###\n')


    #verify if the author index is new or already in DB
    #if new => add to DB
    if(author_seed not in AUTHORSDB):
        AUTHORSDB.append(author_seed)
        writeNewSeedAuthor(author_seed)

    #add the new msg_id to the MSGDB
    MSGDB.append(message_id)
    writeNewMsg(message_id)
    
    #remove the item in the Queue => allow the loader to upload the next msg
    item = Queue.get()

    print("\nPlease enter a command:\n")


def loadArxivDataset(client):
    global Queue

    #load the dataset used to upload messages
    paperId_and_info_and_date_Seed = pd.read_csv('./Data_to_load/paperId_and_info_and_date_Seed.csv')

    citations_with_data= pd.read_csv('./Data_to_load/citations(hep-th)_with_Data.csv')

    TOPOLOGICAL_SORT_df= pd.read_csv('./Data_to_load/TOPOLOGICAL_SORT_df.csv')
    TOPOLOGICAL_SORT_df.sort_values(by='0',inplace = True)
    TOPOLOGICAL_SORT_df.reset_index(drop=True,inplace = True)

    #temp df per associare i NodeId dati dal dataset ai msgId dati dalla tangle
    #utile per tracciare articoli caricati e fare referenze a parents (citazioni)
    caricati = pd.DataFrame(columns=['NodeId','msgIdTangle']) 


    #carico messaggio iniziale a cui i nodi di frontiera faranno riferimento
    genesis_msg = client.message(index = 'GENESIS-ALLEN',data_str='GENESIS ARTICLE')
    genesis_msg_id = genesis_msg['message_id']


    for i in range(len(TOPOLOGICAL_SORT_df)):

        #wait the MQTT reader to finish the processing of the last msg uploaded
        while(Queue.qsize()>0):
            pass

        #info sull'articolo da caricare
        article_id = TOPOLOGICAL_SORT_df.iloc[i]['0']
        author_seed = paperId_and_info_and_date_Seed[paperId_and_info_and_date_Seed['ToNodeId']==article_id]['Seed']
        title = paperId_and_info_and_date_Seed[paperId_and_info_and_date_Seed['ToNodeId']==article_id]['Title']
        date = paperId_and_info_and_date_Seed[paperId_and_info_and_date_Seed['ToNodeId']==article_id]['Date']
    
        #se sei un nodo di frontiera 
        if(article_id not in citations_with_data['FromNodeId'].values):
            message = client.message(index=author_seed.values[0], data_str=str(title.values[0]+'\n Date: '+date.values[0]), parents = [str(genesis_msg_id)])

        else:
            #per ogni parent NodeId, ottengo il relativo msg_id sulla tangle
            parents_node_id = citations_with_data[citations_with_data['FromNodeId'] == article_id]['ToNodeId']
            parents_msg_id = caricati[caricati['NodeId'].isin(parents_node_id.values)]['msgIdTangle']
        
            parents_msg_id = list(parents_msg_id.values)


            #in base al vincolo della tangle, limito i parents ad 8
            if(len(parents_msg_id)>= 9):
                parents_msg_id = parents_msg_id[0:8]
            
            message = client.message(index=author_seed.values[0], data_str=str(title.values[0]+'\n Date: '+date.values[0]), parents = parents_msg_id)

        #aggiungo il messaggio caricato 
        caricati.loc[i,'NodeId'] = article_id
        caricati.loc[i,'msgIdTangle'] = str(message['message_id'])

        #inserisco item e mi metto in attesa che MQTT finisca il processing e liberi la coda
        Queue.put('wait')

        #wait per sync (forse non serve)
        time.sleep(1)

    #salva il DB
    DB_articles_authors.to_csv('./DB_articles_authors.csv',index = False)


def addRowToDB(FromNodeId, From_Author_Seed, ToNodeId):
    #HERE THE 'ToNodeId' VALUE IS THE ID OF THE PARENT MESSAGE CITED BY THE NEW PUB MSG

    global DB_articles_authors

    #each row will be added dinamically to the DB, which init is empty
    # so the row in which will be added is the actual length value
    index_to_add_row = len(DB_articles_authors)
    
    #add the given info
    DB_articles_authors.loc[index_to_add_row,'FromNodeId'] = FromNodeId #aggiungo valori
    DB_articles_authors.loc[index_to_add_row,'From_Author_Seed'] = From_Author_Seed
    DB_articles_authors.loc[index_to_add_row,'ToNodeId'] = ToNodeId


    #retrieve the parent msg author seed cited by the new msg:

    #search in the DB, the message used as parent msg, then get its auth seed
    query = DB_articles_authors[DB_articles_authors['FromNodeId'] == ToNodeId]['From_Author_Seed']
    
    
    #query will contain a row for each citation done by this parent msg (ToNodeId)
    if(len(query)>0):
        To_Author_Seed = np.unique(query.values)[0]
    else:
        #Not available ogni volta che il messaggio usato come parent è 
        #il GENESIS_MSG
        To_Author_Seed = 'Not_available'

    #add the auth seed to the row
    DB_articles_authors.loc[index_to_add_row,'To_Author_Seed'] = To_Author_Seed


#PR computed on the messages 
def computePageRank():
    # DB_articles_authors ha le seguenti colonne:
    # 'FromNodeId' | 'From_Author_Seed' | 'ToNodeId' | 'To_Author_Seed

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

    #estraggo tutti i seed introdotti nella piattaforma
    From_seed_ids = DB_articles_authors.From_Author_Seed.values


    #NB: prendo solo i seed degli autori in From, perche per costruire il grafo
    # aggiungo archi (e non nodi). Quindi aggiungere tutti gli archi =>
    # aggiungere tutti i nodi contenuti nel grafo

    
    #rimuovo i duplicati e lascio solo seed distinti
    unique_seed_ids = np.unique(From_seed_ids)

    print(f'#UNIQUE SEEDS: {len(unique_seed_ids)}')

    #creo grafo vuoto
    D=nx.DiGraph() 

    for i in unique_seed_ids:
        D.add_node(str(i),nodeId = str(i))
    

    #per ogni autore citante
    for citing_seed_author in unique_seed_ids:  
        
        #raccolgo sue citazioni nel DB
        outgoing_citation_from_seed = DB_starting[DB_starting['From_Author_Seed']==citing_seed_author]

        outgoing_citation_from_seed = outgoing_citation_from_seed[outgoing_citation_from_seed['To_Author_Seed']!= 'Not_available']


        #print('OCFS')
        #print(outgoing_citation_from_seed)


        if(len(outgoing_citation_from_seed)>0):

            #invidiuo il num di cit fatte verso ogni altro autore
            outgoing_citation_from_seed = np.unique(outgoing_citation_from_seed.To_Author_Seed.values,return_counts=True)

            #print('OCFS2')
            #print(outgoing_citation_from_seed)

            #inserisco nel grafo l'arco (autore_citante, autore_citato, num citazioni)
            for i in range(len(outgoing_citation_from_seed[0])):
                cited_seed_author = outgoing_citation_from_seed[0][i] #[0] contiene i diversi valori
                
                if(citing_seed_author=='ff9af28010b1a78e3af698c64734cecf907988b927435d9a887a4cdcce248611'):
                    print(f'AUTORI CITATI: {cited_seed_author}')

                num_citations = outgoing_citation_from_seed[1][i] #[1] le occorrenze di tali valori
                D.add_weighted_edges_from([(str(citing_seed_author),str(cited_seed_author),num_citations)])

    nx.write_weighted_edgelist(D, 'graph.csv')



    #ottengo matrice adiacenza da grafo
    #AM_sparse =nx.adjacency_matrix(D) #list storage type
    AM_matrix = nx.adjacency_matrix(D, nodelist=D.nodes()).todense()
    AM_matrix_df = pd.DataFrame(AM_matrix)
    AM_matrix_df.set_index(unique_seed_ids,inplace = True)
    AM_matrix_df.columns = unique_seed_ids

    print(f'ADJM len: {len(AM_matrix)}')
    #print(f'Adjacency Matrix on authors citation: {AM_matrix_df}')
    AM_matrix_df.to_csv('./AM_Matrix.csv')
    #np.savetxt(r'./AM_Matrix.txt', AM_matrix, fmt='%d')

    #calcolo PageRank
    PR = nx.pagerank(D)
    PR_df = pd.DataFrame.from_dict(PR,orient='index') 
    print(f'Page Rank Values: {PR_df}')
    PR_df.to_csv('./PR_df.csv')   


#if client has restarted, the DB_articles_authors is empty, we can reconstruct it
#by reading the MSGDB stored and get all msg_s published  
def buildDBArticlesAuthors():

    global AUTHORSDB

    global DB_articles_authors

    global MSGDB

    print('\nMessages on Tangle\n')

    for i in range (len(MSGDB)):
        message_id = MSGDB[i]

        #for each msg in the MSGDB retrieve its info
        message = client.get_message_data(message_id)

        author_seed = message['payload']['indexation'][0]['index']
        author_seed = (bytes.fromhex(author_seed).decode('utf-8'))
    
        parents = message['parents']


        for parent_id in parents:
            #the parent msg's author_seed will be retrieved, since if a msg is cited => its author is already in the DB 
            addRowToDB(message_id, author_seed, parent_id)
        #print('#######')

    DB_articles_authors.to_csv('./DB_articles_authors2.csv',index = False)

#write a new Author Seed to the DB file
def writeNewSeedAuthor(author_Seed):
    
    file = open("lista_seed_autori.txt", "a")
    file.write(author_Seed+'\n')        
    file.close() 


#write a new msg id to the DB file
def writeNewMsg(msg_id):
    
    file = open("lista_msgDB.txt", "a")
    file.write(msg_id+'\n')        
    file.close() 



#load the index authors list from DB file to a Global variable
def readIndexAuthorsList():
    try: 
        file = open("lista_seed_autori.txt", "r")
        index_authors_list = file.readlines()

        for i in range(len(index_authors_list)):
            index_authors_list[i] = index_authors_list[i].replace("\n", "")

        print(f'Found #{len(index_authors_list)} authors from the file lista_seed_autori.txt \n')
    except:
        
        index_authors_list = []

    return index_authors_list



#load the msg_id list from DB file to a Global variable
def readMsgDB():
    try: 
        file = open("lista_msgDB.txt", "r")
        msg_DB = file.readlines()

        for i in range(len(msg_DB)):
            msg_DB[i] = msg_DB[i].replace("\n", "")

        print(f'Found #{len(msg_DB)} messages from the file lista_msgDB.txt \n')
    except:
        
        msg_DB = []

    return msg_DB




def main():

    print("\n##########")

    global DB_articles_authors

    #if present, load the DB storing msgs and authors of the tangle

    global AUTHORSDB
    AUTHORSDB = readIndexAuthorsList()

    global MSGDB
    MSGDB = readMsgDB()

    #subscribe to MQTT topic    
    client.subscribe_topic('messages',MQTT_callback)
    print('MQTT Subscription')

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