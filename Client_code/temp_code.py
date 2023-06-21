import os 
import hashlib
import pandas as pd


file = open("data_message.txt", "r")
all_of_it = file.read()
print(all_of_it) 
# close the file
file.close()

    