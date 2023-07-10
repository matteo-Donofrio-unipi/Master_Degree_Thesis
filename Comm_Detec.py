import networkx as nx
import pylab as plt
import pandas as pd
import numpy as np
import math
from operator import itemgetter
from timeit import default_timer as timer
# Create blank graph



def scaleValues(E_matrix):
    #calcolo il total weight sul grafo
    W = 0
    for i in E_matrix.columns.values:
        W += E_matrix.loc[i].sum()
    print(f'Total weight: {W}')
    
    def scale(x):
        return x/W
    
    E_matrix = E_matrix.applymap(scale)
    
    return E_matrix


def computeQ(E_matrix):
    Q = 0

    #calcolo Q iniziale 
    for i in E_matrix.columns.values:
        a_i_square = pow(E_matrix.loc[i].values.sum(),2)
        e_ii = E_matrix.loc[i][i]
        Q += e_ii - a_i_square
    
    return Q


def removeDuplicatePairs(pairs_original, matrix):

    #print(f'ORIGINAL : {pairs_original}')

    pairs_copy = pairs_original.copy()

    for i in range(len(pairs_copy)):
        pairs_copy[i] = tuple(sorted(pairs_copy[i]))

    pairs_copy = sorted(pairs_copy)
    #print(f'SORTED: {pairs_copy}')


    duplicates = []

    for i in range(len(pairs_copy)-1):
        if pairs_copy[i] == pairs_copy[i+1]:
            duplicates.append(pairs_copy[i])

    #print(f'DUPLICATES: {duplicates}')


    for i in duplicates:
        pairs_original.remove(i)
        pairs_original.remove(tuple(sorted(i, reverse = True)))


    #print(f'ORIGINAL W/O DUPLICATES : {pairs_original}')


    for i in duplicates:
        if(matrix.loc[i[0]][i[1]] > matrix.loc[i[1]][i[0]]):
            pairs_original.append(i)
        else:
            pairs_original.append(tuple(sorted(i, reverse = True)))

    #print(f'ORIGINAL UPDATED : {pairs_original}')

    return pairs_original
    



def calcolaMerge(E_matrix):   
    
    print(f'Shape starting: {E_matrix.shape}')
    
    #print(f'E_matrix scalata:\n {E_matrix}')
    

    #singola iterazione per calcolare join tra communities
    
    
    #calcolo Q iniziale
    Q = computeQ(E_matrix)
    print(f'Q iniziale: {Q}')

    #definisco df in cui memorizzare DQs
    df = pd.DataFrame(columns=['pair','DQ','newQ'])
    pairs_to_try = []

    #individuo le coppie da testare => coppie con almeno un arco tra loro
    for i in E_matrix.columns.values:

        temp = pd.DataFrame(E_matrix.loc[i])    
        possible_dest = temp[temp[i]>0].index.values

        for j in possible_dest:
            if(i!=j):
                pairs_to_try.append((i,j))
    #print(pairs_to_try)
    
    if(len(pairs_to_try)>0):
        
        pairs_to_try = removeDuplicatePairs(pairs_to_try, E_matrix)

        #calcolo DQ per ogni coppia individuata
        for k in pairs_to_try:
            i = k[0]
            j = k[1]

            a_i = E_matrix.loc[i].values.sum()
            a_j = E_matrix.loc[j].values.sum()

            DQ = 2 * (E_matrix.loc[i][j] - (a_i*a_j))
            #delta_Qs.append(DQ)

            last = len(df)
            df.loc[last,'pair'] = k #aggiungo valori
            df.loc[last,'DQ'] = DQ #aggiungo valori
            df.loc[last,'newQ'] = Q +DQ #aggiungo valori


        #print(f'DQ ottenute dai merge:\n {df}')

        #identifico la coppia che fornisce maggior incremento positivo in Q => Q + DQ maggiore
        winning_pair = df.query('newQ == newQ.max()')['pair'].values[0]

        print(f'Miglior merge: {winning_pair}')


        #genero le nuove colonne merged MA NON LE AGGIUNGO SUBITO
        old_columns = E_matrix.columns.values

        #print(f'old_columns: {old_columns}')


        old_columns = np.delete(old_columns, np.argwhere(old_columns == winning_pair[0]))
        old_columns = np.delete(old_columns, np.argwhere(old_columns == winning_pair[1]))


        new_columns = np.append(old_columns,winning_pair[0]+'-'+winning_pair[1])
        #print(f'new_columns: {new_columns}')

        #genero le nuove rows merged



        #rows to merge
        merged_row = E_matrix.loc[winning_pair[0]] + E_matrix.loc[winning_pair[1]]

        #remove rows of single merged communities
        E_matrix.drop(index = winning_pair[0], inplace=True)
        E_matrix.drop(index = winning_pair[1], inplace=True)

        #add new merged row to matrix
        E_matrix.loc[winning_pair[0]+'-'+winning_pair[1]] = merged_row

        #NOW CHANGE IN THE COLUMNS

        #first compute the list of values to be insterted in the new column
        new_values = []

        #for all rows compute the merged sum value 
        for i in new_columns:
            new_value = E_matrix.loc[i][[winning_pair[0],winning_pair[1]]].sum()
            new_values.append(new_value)

        #remove the single columns of merged comm
        E_matrix = E_matrix.drop([winning_pair[0], winning_pair[1]], axis=1)

        #add the new merged column
        #E_matrix = E_matrix.insert(winning_pair[0]+winning_pair[1], new_values)


        E_matrix[winning_pair[0]+'-'+winning_pair[1]] = new_values

        #print(f'AM finale: \n {E_matrix}')
        print(f'Shape obtained: {E_matrix.shape}')

        Q_finale = computeQ(E_matrix)
        
    else:
        Q_finale = float("NaN")

    
    #CONDIZIONI DI USCITA:
        #1: SE IL NUMERO DI COLONNE < 2 => NON CI SONO JOIN POSSIBILI DA FARE
        
        #2: SE NUM COLONNE > 2 && ESISTONO SOLO CLUSTER CHE NON HANNO COLLEGAMENTI/ARCHI TRA LORO
    
    return E_matrix, [E_matrix.columns.values, Q_finale]




def main():
    DF = pd.read_csv('./AM_Matrix.csv')
    DF.rename(columns={'Unnamed: 0':'From'}, inplace = True)
    DF.set_index('From',inplace = True,drop=True)

    for i in DF.columns.values:
        DF.loc[i][i] = 0


    #DF

    DF = scaleValues(DF)

    clusters_iterations = []

    start = timer()
    
    while(DF.shape[0]>1):
        start = timer()
        DF, clusters = calcolaMerge(DF)
        clusters_iterations.append(clusters)
        end = timer()
        print(f'TIME ELAPSED: {end - start}') 
        if(math.isnan(clusters[-1])):
            break
        else:
            print(f'Q obtained: {clusters[-1]}\n')
            print('\n########\n')
    end = timer()
    print(f'TIME ELAPSED: {end - start}')

    CDF = pd.DataFrame(clusters_iterations)
    CDF.to_csv('Clusters_DF_Computed.csv',index = False)

    #TEMPO IMPIEGATO: 1471.252479662 SEC = 24,5 MIN
    CDF = pd.read_csv('./Clusters_DF_Computed.csv')
    #DF.rename(columns={'Unnamed: 0':'From'}, inplace = True)
    #DF.set_index('From',inplace = True,drop=True)
    CDF

    best_clusterization = CDF.iloc[CDF[CDF['1']==CDF['1'].max()].index[0]]['0']
    clusters_authors = best_clusterization.split("'\n '")
    num_clusters = len(clusters_authors)

    size_of_cluster = []

    for i in clusters_authors:
        if('-' in i):
            size_of_cluster.append(len(i.split("-")))
        else:
            size_of_cluster.append(1)


    Best_Clusters_DF = pd.DataFrame(columns=[['cluster_authors','cluster_size']])
    Best_Clusters_DF['cluster_authors'] = clusters_authors
    Best_Clusters_DF['cluster_size'] = size_of_cluster
    Best_Clusters_DF  


if __name__ == "__main__":
    main()

