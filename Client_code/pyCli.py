import iota_client
import pandas as pd 
import os 
import hashlib


# operation can be 'get_msg', 'send_msg', 'get_balance'

operation = 'get_msg'

########################################



###STANDARD OPERATIONS FOR NODE CONNECTION##############################
print("INIT")

# create a client with a node
client = iota_client.Client(
    nodes_name_password=[['http://0.0.0.0:14265']])

print(client.get_info())

###CHECK OR GENERATE ADDRESSES###

try:
    file = open("seed_address.txt", "r")
    data = file.readlines()
    print(data)
    seed = data[0]
    address = data[1]
    file.close()

    print(f'seed: {seed}')
    print(f'address: {address}')
    

except:
    
    rnd_seed = hashlib.sha256(os.urandom(256)).hexdigest()
    print("New seed generated")
    print(rnd_seed)

    address = client.get_addresses(
    seed=rnd_seed,
    account_index=0,
    input_range_begin=0,
    input_range_end=1,
    get_all=False
    )

    file = open("seed_address.txt", "x")
    file.write(rnd_seed+"\n")
    file.write(address[0][0])
    file.close()

########################################################################à


###GET OPS###
if(operation=='get_msg'):
    print("Message data\n")
    #message = client.get_message_data("14e92174d4882e4c66054bcff5e3979d997f0accd7d0b65cd04488e91efe79d5")
    #message_meta = client.get_message_metadata("14e92174d4882e4c66054bcff5e3979d997f0accd7d0b65cd04488e91efe79d5")

    message = client.find_messages(indexation_keys=["articolo4"])
    message = message[0]
    #print(f'Messages: {message}')

    data = message['payload']['indexation']
    data = data[0] 
    data = data['data']
    #print(type(data))
    text = str(bytearray(data).decode('utf-8'))  

    print(text)

###SEND OPS###
if(operation=='send_msg'):

    file = open("data_message.txt", "r")
    all_of_it = file.read()
    file.close()
    
    data = str(all_of_it)
    print(type(data))
    message = client.message(index='articolo4', data_str=data)
    #print(message)

###GET BALANCE OPS###
if(operation=='get_balance'):
    print(f'get_address_balance() for address {address}')
    print(f'balance: {client.get_address_balance(address)}')




    #print(bytes.fromhex(str((data[0]['data'])[0])).decode('utf-8'))

'''

citations = pd.read_csv('data_message.txt', sep = "\t")


print('message\n')
message = client.message(index='articolo', data_str=str(citations.values))
print(message)

'''




print("END")