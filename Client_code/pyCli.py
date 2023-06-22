import iota_client
import pandas as pd 
import os 
import hashlib


# operation can be 'get_msg_by_author', 'get_msg_by_id', 'send_msg', 'get_balance', 'send_msg_with_parents'
OPERATION = 'get_msg_by_author'


#get messages by index=> author seed
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


def sendMsgWithParents(client, seed, index_key_parent):

    print('dentro la f')

    data_of_payload = getDataFromFile()
    #print(type(data))
    

    #retrieve the message to be used as parent
    message = client.find_messages(indexation_keys=[index_key_parent])
    message = message[0]
    parent_id = message['message_id']
    print(f'Parent id: {parent_id}')


    
    message = client.message(index=seed, data_str=data_of_payload, parents = [str(parent_id)])
    #print(message)




def sendEmptyMsg(client, seed):
    message = client.message(index=seed, data_str='ARTICOLO 1 DATA')
    print(message)




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

    print("\n##########\n")

    # create a client with a node
    client = iota_client.Client(
        nodes_name_password=[['http://0.0.0.0:14265']])

    #print(client.get_info())

    ###CHECK OR GENERATE ADDRESSES###

    try:
        file = open("seed_address.txt", "r")
        data = file.readlines()
        #print(data)
        address = data[0]
        seed = data[1]
        file.close()

        print(f'seed: {seed}')
        print(f'address: {address}')
        

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

        file = open("seed_address.txt", "x")
        file.write(address[0][0]+'\n')
        file.write(seed)        
        file.close()    


    idMsg_to_retrieve = '7d89a17e2a732717c7fd3642c2624a1da64066eef050b056305788ceeca5b203'
    index_author_to_retrieve = 'ad179eb32b067a4eb5d7799a013c245d407ceb2c77600a963c768e6260a01898'

    if(OPERATION=='get_msg_by_id'):
        getMsgById(client, idMsg_to_retrieve)

    elif(OPERATION=='get_msg_by_author'):
        getMsgByAuthor(client, index_author_to_retrieve)
    
    elif(OPERATION=='send_msg'):
        sendEmptyMsg(client, seed)

    elif(OPERATION=='send_msg_with_parents'):
        sendMsgWithParents(client, seed, index_author_to_retrieve)

    elif(OPERATION=='get_balance'):
        getBalance(client, address)
                    

    print("\n##########\n")






if __name__ == "__main__":
    main()


