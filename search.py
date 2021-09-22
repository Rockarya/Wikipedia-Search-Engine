import json
import os
import bisect
import nltk
import math
import time
import sys
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer 
stop_words = set(stopwords.words('english'))
snow_stemmer = SnowballStemmer(language='english')
field_tags = ['t','b','i','c','r','l']         #title,body,infobox,category,references,links in order
field_weights = {}
field_weights['t'] = 100.0 
field_weights['b'] = 1.0
field_weights['i'] = 20.0
field_weights['c'] = 10.0
field_weights['r'] = 0.03 
field_weights['l'] = 0.01


with open("./final_inverted_index/title/title_helper.txt") as f:
    title_helper = json.load(f)   #the stored format id json so just retrieve as list from it
len_title_helper = len(title_helper)
f.close()

with open("./final_inverted_index/search_helper.txt") as f:
    search_helper = json.load(f)
len_search_helper = len(search_helper)
f.close()


# binary search
def bins(l, r, element, array):
    ans=0
    while l<=r:
        m=(l+r)//2
        if array[m]<=element:
            ans=m
            l=m+1
        else:
            r=m-1
    return ans


query_lst = []
f = open(sys.argv[1],'r')
while 1:
    line = f.readline().strip('\n')
    if len(line) == 0:
        break
    query_lst.append(line)


for q in query_lst:
    start_time = time.time()
    query = q.lower()
    query_words = []  
    word_tag={}   #this will be used for field query
    splitted = query.split(':')

    # plain query
    if len(splitted) == 1:
        for word in query.split(' '):
            if word not in stop_words:
                query_words.append(snow_stemmer.stem(word))
    
    else:
        arr = query.split(' ')
        lst = []
        for a in arr:
            temp = a.split(':')
            for b in temp:
                lst.append(b)

        tag = ''
        for elem in lst:
            if elem in field_tags:
                tag = elem
            else:
                if elem not in stop_words:
                    query_words.append(snow_stemmer.stem(elem))
                word_tag[elem]=tag

    doc_score = {}
    doc_ids = set()
    for word in query_words:
        ind = bins(0,len_search_helper-1,word,search_helper)
        ind+=1 #as the files are 1 based-index

        inverted_dict = {}
        file_name = "./final_inverted_index/" + str(ind) + ".txt"
        with open(file_name) as f:
            inverted_dict = json.load(f)  
        f.close()
              
        posting_lst = []  
        if inverted_dict.get(word) != None:
            posting_lst = inverted_dict[word]

        total_docs = len(posting_lst)
        if total_docs > 0:  #handling case when posting list is empty
            docs = {}
            tag_count = {}
            for ch in field_tags:
                tag_count[ch] = 0

            for elem in posting_lst: #title,body,infobox,category,references,links in order
                docs[elem[0]] = {}
                if elem[0] not in doc_ids:
                    doc_score[elem[0]] = 0.0
                doc_ids.add(elem[0])

                for ch in field_tags:
                    docs[elem[0]][ch]=0

                freq = ""
                for ch in elem[1]:
                    if ch in field_tags:
                        tag_count[ch]+=1
                        frequency = int(freq)
                        docs[elem[0]][ch] = frequency
                        freq = ""

                    else:
                        freq += ch

            for elem in posting_lst:
                score = 0.0
                for ch in field_tags:
                    frequency = docs[elem[0]][ch]
                    if frequency != 0:    #if frequency is non-zero then tag count would definetely be non-zero 
                        val = ((field_weights[ch]) * (math.log(1+frequency))*(math.log(total_docs/tag_count[ch])))  
                        if (word_tag.get(word) != None) and (word_tag[word]==ch):        #increasing weights of the token which is present is field query in same tag as we are calculating for
                            val*=100.0
                        score+=val

                doc_score[elem[0]]+=score
                
    score_id = []
    for ids in doc_ids:
        score_id.append((doc_score[ids],ids))

    score_id.sort(reverse = True)
    id_lst = []
    cnt = 0
    for elem in score_id:   
        cnt+=1
        id_lst.append(elem[1])
        if cnt == 10:           #top 10 results
            break

    id_title = []
    for ids in id_lst:
        ind = bins(0,len_title_helper-1,ids,title_helper)
        ind+=1
        file_name = "./final_inverted_index/title/" + str(ind) + ".txt"
        inverted_title ={}
        with open(file_name) as f:
            inverted_title = json.load(f)  
        f.close()
        
        str_id = str(ids)
        if inverted_title.get(str_id) != None:
            id_title.append((ids,inverted_title[str_id]))
        
        
    time_took = time.time() - start_time
    write_in_file = ""
    for elem in id_title:
        write_in_file += (str(elem[0]) + ', ' + elem[1] + '\n')
    write_in_file += (str(time_took) + '\n\n')
    with open("./queries_op.txt",'a') as f:
        f.write(write_in_file)
    f.close()