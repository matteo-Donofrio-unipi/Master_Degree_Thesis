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
global AUTHORSDB

global Queue

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





def MQTT_callback(msg):
    global Queue


    #the msg received is a structured string, it will be converted as dict and 
    #its field will be read 

    #digging in the msg fields
    dict = json.loads(msg)

    message_id = client.get_message_id(str(dict['payload']))

    dict = dict['payload']
    dict = json.loads(dict)

    parents_id = dict['parents']



    dict = dict['payload']
    
    author_seed = str(bytearray(dict['data']['index']).decode('utf-8'))
    data = str(bytearray(dict['data']['data']).decode('utf-8'))

    print('####### \n MQTT msg pub:')
    print(f'MsgId: {message_id}\n')
    print(f'Index author: {author_seed}')
    print(f'Data: {data}\n')
    #print(f'Parents: {parents_id}\n')

    #here, given the published msg we've: msg_id, auth_seed, parents_id

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
    
    item = Queue.get()

    print("\nPlease enter a command:\n")


def loadArxivDataset(client):
    global Queue

    paperId_and_info_and_date_Seed = pd.read_csv('./Data_to_load/paperId_and_info_and_date_Seed.csv')

    citations_with_data= pd.read_csv('./Data_to_load/citations(hep-th)_with_Data.csv')

    TOPOLOGICAL_SORT_df= pd.read_csv('./Data_to_load/TOPOLOGICAL_SORT_df.csv')

    #df temporale per associare i NodeId dati dal dataset ai msgId dati dalla tangle
    #utile per tracciare articoli caricati e fare referenze a parents (citazioni)
    caricati = pd.DataFrame(columns=['NodeId','msgIdTangle']) 

    for i in range(len(TOPOLOGICAL_SORT_df)-1,0,-1):

        while(Queue.qsize()>0):
            pass

        #info sull'articolo da caricare
        article_id = TOPOLOGICAL_SORT_df.iloc[i]['0']
        author_seed = paperId_and_info_and_date_Seed[paperId_and_info_and_date_Seed['ToNodeId']==article_id]['Seed']
        title = paperId_and_info_and_date_Seed[paperId_and_info_and_date_Seed['ToNodeId']==article_id]['Title']
        date = paperId_and_info_and_date_Seed[paperId_and_info_and_date_Seed['ToNodeId']==article_id]['Date']
    
        #controllo le citazioni uscenti dall'articolo da caricare
        outgoing_citations = citations_with_data[citations_with_data['FromNodeId']==article_id]
    
    
        if(len(outgoing_citations)==0): #dal nodo in considerazione non escono citazioni  

            message = client.message(index=author_seed.values[0], data_str=str(title.values[0]+'\n Date: '+date.values[0]))
            #print(caricati.loc[i])
        
        else:
            #se ci sono parents, li ottengo prendendo il sottoinsieme di articoli già
            #caricati il cui NodeId è uguale ai NodeId degli articoli che sto citando
            parents = caricati[caricati['NodeId'].isin(outgoing_citations['ToNodeId'].values)]['msgIdTangle']
            parents = list(parents.values)

            if(len(parents)>= 9):
                parents = parents[0:8]

            message = client.message(index=author_seed.values[0], data_str=str(title.values[0]+'\n Date: '+date.values[0]), parents = parents)

        caricati.loc[i,'NodeId'] = TOPOLOGICAL_SORT_df.iloc[i]['0']
        caricati.loc[i,'msgIdTangle'] = str(message['message_id'])

        Queue.put('wait')

        if(i%100 == 0):
            print(i)


def addRowToDB(FromNodeId, From_Author_Seed, ToNodeId):
    #HERE THE 'ToNodeId' VALUE IS THE ID OF THE PARENT MESSAGE CITED BY THE NEW PUB MSG


    #DB columns: 'FromNodeId', 'From_Author_Seed','ToNodeId', 'To_Author_Seed

    #each row will be added dinamically to the DB, which init is empty
    # so the row in which will be added is the actual length value
    index_to_add_row = len(DB_articles_authors)
    
    #add the given info
    DB_articles_authors.loc[index_to_add_row,'FromNodeId'] = FromNodeId #aggiungo valori
    DB_articles_authors.loc[index_to_add_row,'From_Author_Seed'] = From_Author_Seed
    DB_articles_authors.loc[index_to_add_row,'ToNodeId'] = ToNodeId

    

    #retrieve the parent_seed cited by the new msg:

    #search in the DB, the message used as parent msg, then get its auth seed
    query = DB_articles_authors[DB_articles_authors['FromNodeId'] == ToNodeId]['From_Author_Seed']
    
    
    #query will contain a row for each citation done by this msg
    if(len(query)>0):
        To_Author_Seed = np.unique(query.values)[0]
    else:
        #Not available ogni volta che il messaggio usato come parent è 
        #un messaggio non caricato da un utente della piattaforma 
        # (o di cui nel dataset non citano nessuno )
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
                num_citations = outgoing_citation_from_seed[1][i] #[1] le occorrenze di tali valori
                D.add_weighted_edges_from([(str(citing_seed_author),str(cited_seed_author),num_citations)])

    #ottengo matrice adiacenza da grafo
    AM_sparse =nx.adjacency_matrix(D) #list storage type
    AM_matrix = AM_sparse.todense()
    print(f'Adjacency Matrix on authors citation: {AM_matrix}')
    np.savetxt(r'./AM_Matrix.txt', AM_matrix, fmt='%d')

    #calcolo PageRank
    PR = nx.pagerank(D)
    PR_df = pd.DataFrame.from_dict(PR,orient='index') 
    print(f'Page Rank Values: {PR_df}')
    PR_df.to_csv('./PR_df.csv',index=False)   



#if client has restarted, the DB_articles_authors is empty, we can reconstruct it
#by reading the INDEXAUTHORSLIST stored and get all msg from authors  
def buildDBArticlesAuthors():

    global AUTHORSDB

    print('\nMessages on Tangle\n')

    print(f'LEN AUTHORS (buildDBArticlesAuthors): {len(AUTHORSDB)}')

    for i in range (len(AUTHORSDB)):
        author_seed = AUTHORSDB[i]
        #print(f'author_index: {author_seed}')

        #msgs from a given author
        messages = client.find_messages(indexation_keys=[author_seed])

        for j in range(len(messages)):
            message = messages[j]

            message_id = message['message_id']

            parents = message['parents']

            data = message['payload']['indexation']
            data = data[0] 
            data = data['data']
            data = str(bytearray(data).decode('utf-8')) 

            #print(f'msg_id: {message_id}')
            #print(f'parents: {parents}')
            #print(f'data: {data}')
            #print('---\n')

            #for each citation done by the published msg, we add a row in the DB
        for parent_id in parents:
            #the parent msg's author_seed will be retrieved, since if a msg is cited => its author is already in the DB 
            addRowToDB(message_id, author_seed, parent_id)
        #print('#######')

    DB_articles_authors.to_csv('./DB_articles_authors2.csv',index = False)

#write a new Index Author to the DB file
def writeNewSeedAuthor(author_index):
    
    file = open("lista_seed_autori.txt", "a")
    file.write(author_index+'\n')        
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





# create a client with a node
client = iota_client.Client(
        nodes_name_password=[['http://0.0.0.0:14265']])

#on init create the DB that stores articles and track citations
DB_articles_authors = pd.DataFrame(columns=['FromNodeId', 'From_Author_Seed','ToNodeId', 'To_Author_Seed']) #definisco df


def main():
    #INIT CONNECTION TO NODE AND SEED/ADDRESS RETRIEVING
    global Queue

    Queue = queue.Queue(maxsize=1)

    print("\n##########")


    global AUTHORSDB
    AUTHORSDB = readIndexAuthorsList()
    #subscribe to MQTT topic    
    client.subscribe_topic('messages',MQTT_callback)
    print('MQTT Subscription')

    user_command = ''

    while(user_command!='exit'):
        user_command = input("\nPlease enter a command:\n")
        
        if(user_command == 'print_DB'):
            print(f'Len DB_articles_authors: {len(DB_articles_authors)}')
            for i in range(len(DB_articles_authors)):
                print(DB_articles_authors.iloc[i]+'\n')

        if(user_command == 'PR'):
            computePageRank()

        if(user_command == 'LAD'):
            loadArxivDataset(client)
        


if __name__ == "__main__":
    main()