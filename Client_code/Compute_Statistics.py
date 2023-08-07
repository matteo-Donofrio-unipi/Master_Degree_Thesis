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
import math
from operator import itemgetter
from timeit import default_timer as timer



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







def buildDBArticlesAuthors():

    global DB_articles_authors

    global MSGDB

    global client

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




#PR computed on the messages 
def computePageRank():

    print('Strating PageRank computation...\n')

    #POICHE LE CHIAVI DEVONO AVERE INTESTAZIONE E FINE (BEGIN & END), LASCIO
    #LE INTESTAZIONI PER TUTTA LA COMPUTAZIONE E ONLINE, POI QUANDO FACCIO
    #CALCOLI QUI DENTRO LE RIMUOVO

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

    return PR_df





#Fornito in input un set di ids delle communities che citano un articolo
#Per ogni community, calcola la distanza media tra di essa e tutte le altre.
#Poi viene restituita la distanza media minore.

#Fornito in input un set di ids delle communities che citano un articolo & community autore articolo citato
#Per ogni community, calcola la distanza media tra di essa e tutte le altre.
#Poi viene restituita la distanza media minore.

#viene invocata solo se il numero di communities coinvolte è > 1 => l'articolo è citato da almeno una community diversa da quella del suo autore

def computeMinOfHopAvgDist(communities_coinvolte, max_len, path):
    
    min_avg_dist = max_len #init la distanza minore individuata finora al valore della distanza max
    
    #print(f'COMP, comm_coinvolte: {communities_coinvolte}')
    
    for i in communities_coinvolte: #per ogni community citante
        
        #print(f'I: {i}')
        
        local_avg_dist = 0

        for j in communities_coinvolte: #scandisco le altre communities citanti
            #print(f'J: {j}')
            if(i!=j):
                if(str(j) in path[str(i)]):
                    local_avg_dist += len(path[str(i)][str(j)])-1 #se esiste path tra loro, prendo il num di hop
                    #print(f'LAD1: {local_avg_dist}')
                else:
                    local_avg_dist += max_len #se non esiste path tra loro, assumo #hop massimo nel grafo PERCHE maggiore è la distanza e maggiore è il punteggio assegnato
                    #print(f'LAD2: {local_avg_dist}')
                    
        local_avg_dist = local_avg_dist/(len(communities_coinvolte)-1) #calcolo media distanza tra la community e le altre (che in numero sono len(communities_coinvolte)-1)
            
        #print(f'LAD3: {local_avg_dist}')
            
            
        if(local_avg_dist > 0 and local_avg_dist < min_avg_dist):
            min_avg_dist = local_avg_dist

    return min_avg_dist






def computeAvgOfHopAvgDist(communities_coinvolte, max_len, path):
    

    avg_dist = 0 #init la distanza media
    
    #print(f'COMP, comm_coinvolte: {communities_coinvolte}')
    
    for i in communities_coinvolte: #per ogni community citante
        
        #print(f'I: {i}')
        
        local_avg_dist = 0

        for j in communities_coinvolte: #scandisco le altre communities citanti
            #print(f'J: {j}')
            if(i!=j):
                if(str(j) in path[str(i)]):
                    local_avg_dist += len(path[str(i)][str(j)])-1 #se esiste path tra loro, prendo il num di hop
                    #print(f'LAD1: {local_avg_dist}')
                else:
                    local_avg_dist += max_len #se non esiste path tra loro, assumo #hop massimo nel grafo PERCHE maggiore è la distanza e maggiore è il punteggio assegnato
                    #print(f'LAD2: {local_avg_dist}')
                    
        local_avg_dist = local_avg_dist/(len(communities_coinvolte)-1) #calcolo media distanza tra la community e le altre (che in numero sono len(communities_coinvolte)-1)
            
        #print(f'LAD3: {local_avg_dist}')
            
            
        avg_dist += local_avg_dist

    return avg_dist/len(communities_coinvolte)



def replaceSlash(row):
    return row.replace('\\n','\n')



def CommunitiesGraph(Communities, DB_AA):
#Costruisco grafo avente tanti nodi quante communities. Ogni arco a->b esiste <=> la community a contiene un 
#autore che cita un autore contenuto nella community b
    num_nodi = len(Communities)
    D=nx.DiGraph()

    for i in Communities.index.values:
        D.add_node(str(i),nodeId = str(i))
        
    for i in Communities.index.values: 
        
        inner_authors = Communities.iloc[i]['cluster_authors'].split("*")
        
        for j in Communities.index.values:
            
            if(i!=j):
            
                outer_authors = Communities.iloc[j]['cluster_authors'].split("*")

                temp_df = DB_AA[DB_AA['From_Author_Pub_Key'].isin(inner_authors)]

                temp_df = temp_df[temp_df['To_Author_Pub_key'].isin(outer_authors)]

                #print(temp_df)

                if(len(temp_df) > 0):           

                    D.add_weighted_edges_from([(str(i),str(j),len(temp_df))])

    return D


def computeCommunitiesSplitted(Communities):

    Communities_splitted = pd.DataFrame(columns=['community','authors'])
    Communities_splitted.community = range(0,len(Communities))
    Communities_splitted.set_index('community',inplace = True)


    for i in range(len(Communities)):
        autori = Communities.loc[i]['cluster_authors'].split("*")
        Communities_splitted.loc[i]['authors'] = autori

    return Communities_splitted



def computeArticlesEstimate(a,b,c,d):

    print('\nStarting estimate computation\n')
    
    PR_df = pd.read_csv('./PR_df.csv')
    PR_df.sort_values(by='0',inplace = True, ascending=False)
    PR_df.reset_index(drop = True, inplace = True)
    PR_df.rename(columns={'Unnamed: 0':'Author', '0':'PR_values'}, inplace = True)
    
    DB_AA_originale = pd.read_csv('./DB_articles_authors_built.csv')

    #load the DF containing the communities found
    Communities = pd.read_csv('./Best_Clusters_DF.csv')
    Communities.sort_values(by='cluster_size',inplace = True)
    Communities.reset_index(drop = True, inplace = True)
    repS = np.vectorize(replaceSlash)
    Communities.cluster_authors = repS(Communities.cluster_authors)

    #GRAPH CREATION
    D = CommunitiesGraph(Communities, DB_AA_originale)


    #Poiche il dataset Communities ha, in ogni riga, gli autori separati dal carattere '*', 
    #per rendere efficiente il calcolo in seguito, vengono splittati ora
    Communities_splitted = computeCommunitiesSplitted(Communities)


    #Creo il DF che conterrà la stima di veridicità calcolata per ogni articolo
    stima_veridicita_articoli = pd.DataFrame(columns=['article_id','estimate'])
    stima_veridicita_articoli['article_id'] = MSGDB
    stima_veridicita_articoli.set_index('article_id',inplace = True)


    #Definisco i valori comuni necessari al calcolo

    #PR normalization---
    PR_Max = PR_df.query('PR_values == PR_values.max()')['PR_values'].values[0]
    PR_Min = PR_df.query('PR_values == PR_values.min()')['PR_values'].values[0]
    PR_Difference = PR_Max - PR_Min

    #max_cit_entranti---
    genesis_tx_node_id = DB_AA_originale[DB_AA_originale['To_Author_Pub_key']=='Not_available']['ToNodeId'].values[0]
    #RIMUOVO LE CITAZIONI INIZIALI FATTE VERSO LA GENESIS TX DAGLI ARTICOLI DI FRONTIERA
    DB_AA_originale_senza_genesis = DB_AA_originale[DB_AA_originale['ToNodeId']!= genesis_tx_node_id]
    res = DB_AA_originale_senza_genesis.groupby('To_Author_Pub_key')['From_Author_Pub_Key'].count()
    cit_entranti_max = res.max()
    cit_entranti_min = res.min()
    cit_entranti_Difference = cit_entranti_max - cit_entranti_min

    #max_len_#_hop---
    path = dict(nx.all_pairs_dijkstra_path(D))
    list_of_len = []
    max_len = 0
    min_len = len(D.nodes)
    for node in D.nodes:
        for paths in path[node]:
            local_len = len(path[node][paths])-1
            list_of_len.append(local_len)
            if(local_len > max_len):
                max_len = local_len
            elif(local_len < min_len):
                min_len = local_len


    len_Difference = max_len - min_len

    #CALCOLO EFFETTIVO DELLA STIMA DI VERIDICITA DI OGNI ARTICOLO

    stima = []

    for m in MSGDB:

        autore = DB_AA_originale[DB_AA_originale['FromNodeId']==m]['From_Author_Pub_Key'].values[0]
        PR_autore = PR_df[PR_df['Author']==autore]['PR_values'].values[0]
        num_cit_entranti = len(DB_AA_originale[DB_AA_originale['To_Author_Pub_key']==autore])
        
        if(num_cit_entranti > 0):
        
            autori_citanti = DB_AA_originale[DB_AA_originale['To_Author_Pub_key']==autore]['From_Author_Pub_Key'].values


            AVG_PR_autori_citanti = PR_df[PR_df['Author'].isin(autori_citanti)]['PR_values'].values.mean()

            #print(autori_citanti)

            #calcolo la distanza tra le communities (insieme composto da community autore citato e communities autori citanti)

            communities_coinvolte = []
            
            autori_coinvolti = autori_citanti 
            autori_coinvolti = np.append(autori_coinvolti,autore)
            
            #in questo modo insierisco anche la community dell'autore dell'articolo 
            #=> calcolo la distanza media tra tutte le communities coinvolte, quelle dell'autore citato e citante 

            for i in autori_coinvolti:
                for j in range(len(Communities_splitted)):
                    if(i in Communities_splitted.loc[j]['authors'] ):
                        communities_coinvolte.append(j)

            communities_coinvolte = np.unique(communities_coinvolte) #rimuovo duplicati
            
            #len(communities_coinvolte) SEMPRE >=1, poiche contiene almeno la community dell'autore citato, piu quelle citanti
            

            #len(communities_coinvolte) = 1 quando la community che cita è la stessa dell'autore che ha scritto articolo 
            if(len(communities_coinvolte)==1):
            
                stima_veridicita_articoli.loc[m]['estimate'] = a*((PR_autore-PR_Min)/PR_Difference) + b*((num_cit_entranti-cit_entranti_min)/cit_entranti_Difference) + c*((AVG_PR_autori_citanti-PR_Min)/PR_Difference)  
            
            else:

                Min_of_hop_avg_dist = computeMinOfHopAvgDist(communities_coinvolte, max_len, path)
                
                if(Min_of_hop_avg_dist<1):
                    print('ERROR')
                    print(Min_of_hop_avg_dist)

                stima_veridicita_articoli.loc[m]['estimate'] = a*((PR_autore-PR_Min)/PR_Difference) + b*((num_cit_entranti-cit_entranti_min)/cit_entranti_Difference) + c*((AVG_PR_autori_citanti-PR_Min)/PR_Difference) + d*((Min_of_hop_avg_dist-min_len)/len_Difference)  
        
        else:
            stima_veridicita_articoli.loc[m]['estimate'] = a*((PR_autore-PR_Min)/PR_Difference)        
    
    
    stima_veridicita_articoli.to_csv('./stima_veridicita_articoli.csv')

    print(stima_veridicita_articoli.estimate.max())
    print(stima_veridicita_articoli.estimate.mean())
    print(stima_veridicita_articoli.estimate.min())

    print('Computation of estimation finished\n')

    return stima_veridicita_articoli



#########################
# NB: nel calcolo della stima, gestisco i vari casi usando parti di formula (solo a*, a+b+c, a+b+c+d).
# Perche se avessi gestito i vari casi nella funzione computeMinOfHopAvgDist(), quando non c'erano citazioni restituiva 0
# ma poiche tale valore calcolato va normalizzato, veniva fuori (0-min_len)/len_Difference
# che generava stime negative => errore.
#########################
    

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












client = iota_client.Client(
    nodes_name_password=[['http://0.0.0.0:14265']])

MSGDB = readMsgDB()

DB_articles_authors = pd.DataFrame(columns=['FromNodeId', 'From_Author_Pub_Key','ToNodeId', 'To_Author_Pub_key'])

PR_df = ''







def main():
    if(len(MSGDB)>0):
        global DB_articles_authors

        global PR_df

        DB_articles_authors = buildDBArticlesAuthors()

        user_command = ''

        while(user_command!='exit'):
            user_command = input("\n$ Please enter a command:\n")
        
            if(user_command == 'PR'):
                PR_df = computePageRank()
            
            elif(user_command == 'AE'):
                estimate_df =  computeArticlesEstimate(0.2,0.2,0.2,0.4)








if __name__ == "__main__":
    main()