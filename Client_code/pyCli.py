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

    parents = message['parents']
    print(f'parents: {parents}')

    print(text)
    
#get messaged by messageId => unique id of this specific message
def getMsgById(client, id_Message):

    print('Message wholw\n')
    message = client.get_message_data(id_Message)
    print(message)

    parents = message['parents']

    author_seed = message['payload']['indexation'][0]['index']

    
    author_seed = (bytes.fromhex(author_seed).decode('utf-8'))


    data = message['payload']['indexation']
    data = data[0] 
    data = data['data']
    #print(type(data))
    text = str(bytearray(data).decode('utf-8'))  
    
    print("\nMessage Data\n")
    print(text)

    print('Parents:\n')
    for i in parents:
        print(i)

    print('Seed author:\n')
    print(type(author_seed))
    print(author_seed)

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
                        
        elif(user_command == 'LAD'):
            loadArxivDataset(client)

        print("\n##########\n")






if __name__ == "__main__":
    main()


