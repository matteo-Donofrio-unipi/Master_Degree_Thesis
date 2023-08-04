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





#write a new msg id to the DB file
def writePrivateKey(private_key_pem_string):
    
    file = open("private_key_stored.txt", "w")
    file.write(private_key_pem_string)        
    file.close()

    print('Private key stored\n')



#load the msg_id list from DB file to a Global variable
def readPrivateKey():
    print('Checking for private key stored...\n')
    try: 
        file = open("private_key_stored.txt", "r")
        private_key_pem_string = file.read()
        #private_key_pem_string = private_key_pem_string[0]

        #print('READPK1')
        #print(private_key_pem_string)

        #for i in range(len(indices_topics)):
        #    indices_topics[i] = indices_topics[i].replace("\n", "")

        print(f'Found an existing private key \n')

        file.close() 
    except:
        private_key = rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        )

        private_key_pem_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.TraditionalOpenSSL,
        encryption_algorithm=serialization.NoEncryption()
        )

        print(f'Generated a new private key \n')

        private_key_pem_string = private_key_pem_bytes.decode()

        #print('READPK2')
        #print(private_key_pem_string)

        writePrivateKey(private_key_pem_string)
    
    return private_key_pem_string



def sendMSG(private_key, public_pem_string):

    index_topic = input("\n$ Insert the index topic \n")
    index_topic = '#'+str(index_topic)

    article_text = input("\n$ Insert the article text \n")
    
    message_txt_data_string = str(article_text)
    message_txt_data_bytes = str.encode(message_txt_data_string)

    signature = private_key.sign(
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


    num_parents = 0

    while(num_parents <= 0 or num_parents > 8):

        num_parents = input("\n$ How many messages you want to cite? (min 1, max 8) \n")
        num_parents = int(num_parents)


    parents = []
    for i in range(num_parents):

        parent = input(f"\n$ Insert the {i+1} message id to cite \n")
        parent = str(parent)

        parents.append(parent)


    message = client.message(index=str(index_topic), data_str=message_data, parents = parents)
        
    print('\nArticle correctly loaded on the platform\n')
    print(f'Message id published: {message["message_id"]}\n')
    print('################################')




def getMsgByIndex():

    index_topic = input('\n$ Insert the topic index you want to search\n')
    index_topic = '#'+str(index_topic)

    messages = client.find_messages(indexation_keys=[index_topic])
    
    print('The messages found are the following:\n')

    for i in range (len(messages)):

        #take each single message
        message = messages[i]
        #print(f'Messages: {message}')

        #retrieve the id and the data contained in the payload
        msg_id = message['message_id']
        data = message['payload']['indexation']
        data = data[0] 
        data = data['data']

        data = str(bytearray(data).decode('utf-8'))

        data_splitted = data.split('#')

        author_pub_key_string = data_splitted[1]

        signature_string = data_splitted[2]

        text_data_string = data_splitted[3]


        text_data_bytes = str.encode(text_data_string)

        signature_bytes = bytes.fromhex(signature_string)

        author_pub_key_bytes = str.encode(author_pub_key_string)



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

            print(f'MESSAGE ID: {msg_id} \n')
            print(f'DATA: {text_data_string}')
            print("\n\n")
            print('---')

        except InvalidSignature:
            print()
          
    print('End of messages retrieved:\n')
        



# create a client with a node
client = iota_client.Client(
    nodes_name_password=[['http://0.0.0.0:14265']])


def main():
    print("\n##########")
    print('Starting init operation for setup...\n')

    private_key_pem_string = readPrivateKey()

    private_key_pem_bytes = private_key_pem_string.encode()

    private_key = serialization.load_pem_private_key(
        private_key_pem_bytes,
        password=None,
    )

    public_key = private_key.public_key()

    public_pem_string = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo
    ).decode()





    user_command = ''

    while(user_command!='exit'):
        user_command = input("\n$ Please enter a command:\n")
        
        if(user_command == 'GMBI'):
            getMsgByIndex()

        if(user_command == 'SM'):
            sendMSG(private_key, public_pem_string)

    






if __name__ == "__main__":
    main()

