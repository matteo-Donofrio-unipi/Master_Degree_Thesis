import iota_client
import pandas as pd 
import os 
import hashlib


# user_command can be 'get_msg_by_author', 'get_msg_by_id', 'send_msg',
# 'get_balance', 'send_msg_with_parents_by_author', 'send_msg_with_parents_by_id'
#  'get_all_author_msgs', 'load_arxiv_dataset'


#get messages by author index (author seed)
def getMsgByAuthor(client, index_key_author):

    print("Message data\n")

    message = client.find_messages(indexation_keys=[index_key_author])
    message = message[0]
    #print(f'Messages: {message}')

    data = message['payload']['indexation']
    data = data[0] 
    data = data['data']
    #print(type(data))
    text = str(bytearray(data).decode('utf-8'))  

    print(text)
    
#get messaged by messageId => unique id of this specific message
def getMsgById(client, id_Message):

    print("Message Metadata\n")
    message_meta = client.get_message_metadata(id_Message)
    print(message_meta)

    message = client.get_message_data(id_Message)
    data = message['payload']['indexation']
    data = data[0] 
    data = data['data']
    #print(type(data))
    text = str(bytearray(data).decode('utf-8'))  
    
    print("\nMessage Data\n")
    print(text)


#retrieve all the messages having the specified index author
def getAllAuthorMessages(client, index_key_author):

    print(f"# All messages having the author index: {index_key_author}\n")

    messages = client.find_messages(indexation_keys=[index_key_author])


    for i in range (len(messages)):

        #take each single message
        message = messages[i]
        #print(f'Messages: {message}')

        #retrieve the id and the data contained in the payload
        msg_id = message['message_id']
        data = message['payload']['indexation']
        data = data[0] 
        data = data['data']
        #print(type(data))
        text = str(bytearray(data).decode('utf-8'))  

        print(f'MESSAGE ID: {msg_id} \n')
        print(f'DATA: {text}')
        print("\n\n")
        print('---')



#send a msg specifiying, as single parent msg to reference, 
# any msg published by a given author index  
def sendMsgWithParentsByAuthor(client, seed, index_key_parent):


    data_of_payload = getDataFromFile()
    #print(type(data))
    

    #retrieve the message to be used as parent
    message = client.find_messages(indexation_keys=[index_key_parent])
    message = message[0]
    parent_id = message['message_id']
    print(f'Parent id: {parent_id}')


    
    message = client.message(index=seed, data_str=data_of_payload, parents = [str(parent_id)])
    #print(message)


#send a msg specifiying, as single parent msg to reference, 
# the id of a msg  
def sendMsgWithParentsById(client, seed, id_parent):

    data_of_payload = getDataFromFile()
    #print(type(data))
    
    message = client.message(index=seed, data_str=data_of_payload, parents = [str(id_parent)])
    #print(message)



#send a msg without specify the parents (the node will select them for us)
def sendEmptyMsg(client, seed):
    message = client.message(index=seed, data_str='ARTICOLO 4 DATA')
    print(message)

    id = message['message_id']
    print(id)


def loadArxivDataset(client):
    paperId_and_info_and_date_Seed = pd.read_csv('paperId_and_info_and_date_Seed.csv')

    citations_with_data= pd.read_csv('citations(hep-th)_with_Data.csv')

    TOPOLOGICAL_SORT_df= pd.read_csv('TOPOLOGICAL_SORT_df.csv')

    caricati = pd.DataFrame(columns=['NodeId','msgIdTangle']) 

    for i in range(len(TOPOLOGICAL_SORT_df)-1,0,-1):

        #info sull'articolo da caricare
        article_id = TOPOLOGICAL_SORT_df.iloc[i]['0']
        author_seed = paperId_and_info_and_date_Seed[paperId_and_info_and_date_Seed['ToNodeId']==article_id]['Seed']
        title = paperId_and_info_and_date_Seed[paperId_and_info_and_date_Seed['ToNodeId']==article_id]['Title']
        date = paperId_and_info_and_date_Seed[paperId_and_info_and_date_Seed['ToNodeId']==article_id]['Date']
    
        #controllo le citazioni uscenti dall'articolo da caricare
        sub_df = citations_with_data[citations_with_data['FromNodeId']==TOPOLOGICAL_SORT_df.iloc[i]['0']]
    
    
    
        if(len(sub_df)==0): #dal nodo in considerazione non escono citazioni  

            message = client.message(index=author_seed.values[0], data_str=str(title.values[0]+'\n Date: '+date.values[0]))
            #print(caricati.loc[i])
        
        else:
            parents = caricati[caricati['NodeId'].isin(sub_df['ToNodeId'].values)]['msgIdTangle']
            parents = list(parents.values)

            if(len(parents)>= 9):
                parents = parents[0:8]

            message = client.message(index=author_seed.values[0], data_str=str(title.values[0]+'\n Date: '+date.values[0]), parents = parents)

        caricati.loc[i,'NodeId'] = TOPOLOGICAL_SORT_df.iloc[i]['0']
        caricati.loc[i,'msgIdTangle'] = str(message['message_id'])

        if(i%100 == 0):
            print(i)


def getBalance(client, address):
    print(f'get_address_balance() for address {address}')
    print(f'balance: {client.get_address_balance(address)}')


### FUNCTIONS ACCESSING FILES ###

def getDataFromFile():
    #prepare the data to be included in the payload
    file = open("data_message.txt", "r")
    all_of_it = file.read()
    file.close()
    
    data = str(all_of_it)

    return data




def getLastAutoincrementIndex():
    #retrieve the last autoincremented index to be used or setup it to 0
    try:
        file = open("autoincrement_index.txt", "r")
        data = file.readlines()
        #print(data)
        last_index = data[0]
        file.close()

        

    except:
    
        file = open("autoincrement_index.txt", "w")
        file.write('0')
        last_index = '0'
        file.close()
    
    print(f'last index to be used: {last_index}')
    return last_index


def writeLastAuotoincrementIndex(newIndex):
    file = open("autoincrement_index.txt", "w")
    file.write(str(newIndex))
    file.close()

    print(f'last index written: {newIndex}')







def main():
    #INIT CONNECTION TO NODE AND SEED/ADDRESS RETRIEVING

    print("\n##########")

    # create a client with a node
    client = iota_client.Client(
        nodes_name_password=[['http://0.0.0.0:14265']])

    #print(client.get_info())

    ###CHECK OR GENERATE ADDRESSES###

    try:
        file = open("address_seed.txt", "r")
        data = file.readlines()
        #print(data)
        address = data[0]
        seed = data[1]
        file.close()

        print(f'My seed: {seed}\n##########')

        #print(f'address: {address}')
        

    except:
        
        seed = hashlib.sha256(os.urandom(256)).hexdigest()
        print("New seed generated")
        print(seed)

        address = client.get_addresses(
        seed=seed,
        account_index=0,
        input_range_begin=0,
        input_range_end=1,
        get_all=True
        )
        print(address)

        file = open("address_seed.txt", "x")
        file.write(address[0][0]+'\n')
        file.write(seed)        
        file.close()    


    command_list = 'get_msg_by_author, get_msg_by_id, get_all_author_msgs, send_msg, send_msg_with_parents_by_author, send_msg_with_parents_by_id\n'
    print('Command List:\n')
    print(command_list)

    user_command = ''

    while(user_command != 'exit'):

        user_command = input("Please enter a command:\n")

        if(user_command=='get_msg_by_id'):
            idMsg = input("Please enter a message id:\n")
            getMsgById(client, idMsg)

        elif(user_command=='get_msg_by_author'):
            index_author = input("Please enter an author index:\n")
            getMsgByAuthor(client, index_author)

        elif(user_command=='get_balance'):
            getBalance(client, address)
        
        elif(user_command=='get_all_author_msgs'):
            index_author = input("Please enter an author index:\n")
            getAllAuthorMessages(client, index_author)

        elif(user_command == 'send_msg_with_parents_by_id'):
            idMsg = input("Please enter a message id:\n")
            sendMsgWithParentsById(client, seed, idMsg)
        
        elif(user_command=='send_msg'):
            sendEmptyMsg(client, seed)

        elif(user_command=='send_msg_with_parents_by_author'):
            index_author = input("Please enter an author index:\n")
            sendMsgWithParentsByAuthor(client, seed, index_author)
                        
        elif(user_command == 'load_arxiv_dataset'):
            loadArxivDataset(client)

        print("\n##########\n")






if __name__ == "__main__":
    main()


