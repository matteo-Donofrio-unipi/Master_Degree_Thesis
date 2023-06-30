import os 
import pandas as pd
import binascii
import iota_client
import hashlib
import json
import csv
import numpy as np
import networkx as nx

INDEXAUTHORSLIST = []


# create a client with a node
client = iota_client.Client(
        nodes_name_password=[['http://0.0.0.0:14265']])

DB_articles_authors = pd.DataFrame(columns=['FromNodeId', 'From_Author_Seed','ToNodeId', 'To_Author_Seed']) #definisco df


def MQTT_callback(msg):
    global INDEXAUTHORSLIST
    #the msg received is a structured string, it will be converted as dict and 
    #its field will be read 

    #digging in the msg fields
    dict = json.loads(msg)

    message_id = client.get_message_id(str(dict['payload']))

    dict = dict['payload']
    dict = json.loads(dict)

    parents_id = dict['parents']



    dict = dict['payload']
    
    author_index = str(bytearray(dict['data']['index']).decode('utf-8'))
    data = str(bytearray(dict['data']['data']).decode('utf-8'))

    print('MQTT msg pub:')
    print(f'MsgId: {message_id}\n')
    print(f'Index author: {author_index}')
    print(f'Data: {data}\n')
    print(f'Parents: {parents_id}\n')
    
    for parent_id in parents_id:
        addRowToDB(message_id, author_index, parent_id)
    print('###\n')


    #add new msg received in the DB

    

    #verify if the author index is new or already in DB
    #if new => add to DB
    '''
    if(author_index not in INDEXAUTHORSLIST):
        INDEXAUTHORSLIST.append(author_index)
        writeNewIndexAuthor(author_index)
    '''


def addRowToDB(FromNodeId, From_Author_Seed, ToNodeId):
    #'FromNodeId', 'From_Author_Seed','ToNodeId', 'To_Author_Seed

    index_to_add_row = len(DB_articles_authors)
    
    DB_articles_authors.loc[index_to_add_row,'FromNodeId'] = FromNodeId #aggiungo valori
    DB_articles_authors.loc[index_to_add_row,'From_Author_Seed'] = From_Author_Seed
    DB_articles_authors.loc[index_to_add_row,'ToNodeId'] = ToNodeId

    


    query = DB_articles_authors[DB_articles_authors['FromNodeId'] == ToNodeId]['From_Author_Seed']
    if(len(query)>0):
        To_Author_Seed = np.unique(query.values)[0]
    else:
        To_Author_Seed = 'Not available'

    DB_articles_authors.loc[index_to_add_row,'To_Author_Seed'] = To_Author_Seed


    
def computePageRankInputMatrixAndGraph():
    
    # DB_articles_authors ha le seguenti colonne:
    # 'FromNodeId' | 'From_Author_Seed' | 'ToNodeId' | 'To_Author_Seed

    DB_starting = DB_articles_authors #make a copy (snapshot)

    #estraggo tutti i seed introdotti nella piattaforma
    From_seed_ids = DB_articles_authors.From_Author_Seed.values

    
    #rimuovo i duplicati e lascio solo seed distinti
    unique_seed_ids = np.unique(From_seed_ids)

    #creo grafo vuoto
    D=nx.DiGraph() 

    #per ogni autore citante
    for citing_seed_author in unique_seed_ids:  
        
        #raccolgo sue citazioni nel DB
        outgoing_citation_from_seed = DB_starting[DB_starting['From_Author_Seed']==citing_seed_author]

        if(len(outgoing_citation_from_seed>0)):

            #invidiuo il num di cit fatte verso ogni altro autore
            outgoing_citation_from_seed = np.unique(outgoing_citation_from_seed.To_Author_Seed.values,return_counts=True)

            #inserisco nel grafo l'arco (autore_citante, autore_citato, num citazioni)
            for i in range(len(outgoing_citation_from_seed[0])):
                cited_seed_author = outgoing_citation_from_seed[0][i]
                num_citations = outgoing_citation_from_seed[1][i]
                D.add_weighted_edges_from([(str(citing_seed_author),str(cited_seed_author),num_citations)])

    #ottengo matrice adiacenza da grafo
    AM_sparse =nx.adjacency_matrix(D) #list storage type
    AM_matrix = AM_sparse.todense()
    print(AM_matrix)

    #calcolo PageRank
    PR = nx.pagerank(D)
    PR_df = pd.DataFrame.from_dict(PR,orient='index') 
    print(PR_df)
       




def getAllMessages():
    global INDEXAUTHORSLIST

    print('\nMessages on Tangle\n')

    for i in range (len(INDEXAUTHORSLIST)):
        author_index = INDEXAUTHORSLIST[i]
        print(f'author_index: {author_index}')

        messages = client.find_messages(indexation_keys=[author_index])

        for j in range(len(messages)):
            message = messages[j]

            message_id = message['message_id']

            parents = message['parents']

            data = message['payload']['indexation']
            data = data[0] 
            data = data['data']
            data = str(bytearray(data).decode('utf-8')) 

            print(f'msg_id: {message_id}')
            print(f'parents: {parents}')
            print(f'data: {data}')
            print('---\n')
        print('#######')

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


    global INDEXAUTHORSLIST
    INDEXAUTHORSLIST = readIndexAuthorsList()
    #subscribe to MQTT topic    
    client.subscribe_topic('messages',MQTT_callback)
    print('MQTT Subscription')

    user_command = ''

    while(user_command!='exit'):
        user_command = input("Please enter a command:\n")

        if(user_command == 'get_all_messages'):
            getAllMessages()
        
        if(user_command == 'print_DB'):
            for i in range(len(DB_articles_authors)):
                print(DB_articles_authors.iloc[i])
        


if __name__ == "__main__":
    main()