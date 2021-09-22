
# increase the limit of number of files u can keep open for a time(ulimit -n 2048)
import os
from queue import PriorityQueue
import json
import sys

if os.path.isdir("final_inverted_index") == False:
    os.mkdir("final_inverted_index") 

# making directory named title to store all the title tags in small files
if os.path.isdir("./final_inverted_index/title") == False:
    os.mkdir("./final_inverted_index/title") 


pq = PriorityQueue()
total_files = 1
line = {}   #this is storing the first line of each file
file_num = {}
while 1: 
    file_name = "./inverted_index/" + str(total_files) + ".txt"
    if not os.path.isfile(file_name):
        break
    
    file_num[total_files] = open(file_name, 'r')
    line[total_files] = file_num[total_files].readline().strip('\n') 
    pq.put([eval(line[total_files]),total_files])            #converting to json format for ease of performing merge algorithm and pushing cnt-1
    total_files+=1

file_cnt = 1
sz_cnt = 0
inverted_dict = {}
words = []
total_token_count = 0
while not pq.empty():
    if sz_cnt >= 1500000:   #taking less size so that merged files do not overshoot in size because of large posting lists(average file size is around 5MB)
        file_name = "./final_inverted_index/" + str(file_cnt) + ".txt"
        with open(file_name,'w') as f:
            f.write(json.dumps(inverted_dict))
        f.close()
        file_cnt+=1
        total_token_count+=len(inverted_dict)
        sz_cnt = 0
        inverted_dict.clear()
        
    var = pq.get()
    word = var[0][0]
    if sz_cnt == 0:         #when we we will be writing our first word in file
        words.append(word)
    lst = var[0][1]
    file_num[var[1]]
    line[var[1]] = file_num[var[1]].readline().strip('\n') 
    if len(line[var[1]]) != 0:
        pq.put([eval(line[var[1]]),var[1]])

    while not pq.empty() and pq.queue[0][0][0]==word:
        var = pq.get()
        line[var[1]] = file_num[var[1]].readline().strip('\n') 
        if len(line[var[1]]) != 0:
            pq.put([eval(line[var[1]]),var[1]])
        
        lst.extend(var[0][1])
    
    inverted_dict[word] = lst
    sz_cnt += sys.getsizeof(word)
    sz_cnt += sys.getsizeof(lst)
    

if sz_cnt != 0:  
    file_name = "./final_inverted_index/" + str(file_cnt) + ".txt"
    with open(file_name,'w') as f:
        f.write(json.dumps(inverted_dict))
    total_token_count+=len(inverted_dict)
    f.close()
    
with open("./final_inverted_index/search_helper.txt",'w') as f:
    f.write(json.dumps(words))
f.close()

inverted_dict.clear()
words.clear()
line.clear()
file_num.clear()
lst.clear()