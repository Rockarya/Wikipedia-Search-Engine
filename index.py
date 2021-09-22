import xml.etree.ElementTree as ET
import time
import os
import re
import json
from nltk.corpus import stopwords
from nltk.stem.snowball import SnowballStemmer 
from string import ascii_lowercase

#pathWikiXML = '/home/aryan/IRE/mini-proj-phase-2/data/200mbdata.txt'
pathWikiXML = '/home/aryan/IRE/mini-proj-phase-2/data/big_data.txt'
#also set ulimit -n 2048 to increase number of open files
# pathWikiXML = sys.argv[1]

if os.path.isdir("inverted_index") == False:
    os.mkdir("inverted_index") 

# create the final_inverted_index folder(to store the title_tags file)
if os.path.isdir("final_inverted_index") == False:
    os.mkdir("final_inverted_index") 

if os.path.isdir("./final_inverted_index/title") == False:
    os.mkdir("./final_inverted_index/title") 

# list of all stop words, choosing right library for stopping words can significantly reduce indexing time
stop_words_list = set(stopwords.words('english'))
for ch in ascii_lowercase:      #adding one letter words here
    stop_words_list.add(ch)

snow_stemmer = SnowballStemmer(language='english')
pattern = re.compile('\W')

# checking if string contains only ascii characters
def is_ascii(s):
    return all(ord(c) < 128 for c in s)

# Nicely formatted time string
def hms_string(sec_elapsed):
    h = int(sec_elapsed / (60 * 60))
    m = int((sec_elapsed % (60 * 60)) / 60)
    s = sec_elapsed % 60
    return "{}:{:>02}:{:>05.2f}".format(h, m, s)

# stripping the text with the after the given character
def strip_tag_name(t, ch):
    idx = t.rfind(ch)
    if idx != -1:
        t = t[idx + 1:]
    return t


def process_text(text, mapping):
    global total_tokens, doc_words
    
    # this \W thing is for removing alphanumerics
    string = re.sub(pattern, ' ', text)
    words = []
    for word in string.split():
        for woo in word.split('_'):
            words.append(woo)  #some words have _ character, which was not removed
    
    filtered_words = []
    for word in words:
        if is_ascii(word):   #if the word has only ascii characters
            f1 = 0 
            f2 = 0
            for ch in word:
                if ch=='0' or ch=='1' or ch=='2' or ch=='3' or ch=='4' or ch=='5' or ch=='6' or ch=='7' or ch=='8' or ch=='9':  
                    f1 = 1
                else:
                    f2 = 1
            
            if f1==1 and f2==0 and len(word)<5:
                filtered_words.append(word)
            elif f1==0 and f2==1 and (word not in stop_words_list): 
                if cache_stemming.get(word) == None:
                    stemmed_word = snow_stemmer.stem(word)
                    filtered_words.append(stemmed_word)
                    cache_stemming[word] = stemmed_word
                else:
                    filtered_words.append(cache_stemming[word])

                    
    for word in filtered_words:
        if mapping.get(word) == None:
            mapping[word]=0
        
    for word in filtered_words:
        mapping[word]+=1
        doc_words.add(word)
        
    filtered_words.clear()
    

def add_to_the_list(title_words,body_words,infobox_words,category_words,references_words,links_words,id):
    global inverted_dict,keyword_set, doc_words
    
    for word in doc_words:
        string=""       # storing in the order:- id,title,body,infobox,category,references,links in order
        if word not in keyword_set:
            inverted_dict.setdefault(word,[])
            
        if title_words.get(word) != None:
            string+=str(title_words[word])
            string+='t'
            
        if body_words.get(word) != None:
            string+=str(body_words[word])
            string+='b'
    
        if infobox_words.get(word) != None:
            string+=str(infobox_words[word])
            string+='i'
            
        if category_words.get(word) != None:
            string+=str(category_words[word])
            string+='c'
            
        if references_words.get(word) != None:
            string+=str(references_words[word])
            string+='r'

        if links_words.get(word) != None:
            string+=str(links_words[word])
            string+='l'
            
        inverted_dict[word].append((id,string))
        keyword_set.add(word)
        
    
# Actual code starts here 
start_time = time.time()
title_words = {}  
body_words = {}
infobox_words = {}
category_words = {}
references_words = {}
links_words = {}
keyword_set = set()
doc_words = set()
cache_stemming = {}
inverted_dict = {}
title_tags = {} #this will store all the title tags with their doc ids
doc_count = 0
file_count = 1
t_cnt = 0
title_file_count = 1
title_ids = []


for event, elem in ET.iterparse(pathWikiXML, events=('start', 'end')):
    tname = strip_tag_name(elem.tag,'}')

    if event == 'start':
        if tname == 'page':
            title = ''
            id = -1
            redirect = ''
            inrevision = False
            ns = 0
            
        elif tname == 'revision':
            # Do not pick up on revision id's
            inrevision = True
            
        elif tname == "title" and elem.text!=None:   #for some case this title is coming to be None though it exists
            title = elem.text
            process_text(title, title_words)

        elif tname == 'id' and not inrevision and elem.text!=None:
            id = int(elem.text)
            if t_cnt == 0:
                title_ids.append(id)
            title_tags[id]=title
            t_cnt+=1
            if t_cnt == 50000:
                print("title file count: ",title_file_count)
                t_cnt = 0
                filename = "./final_inverted_index/title/" + str(title_file_count) + ".txt"
                with open(filename,'w') as f:
                    f.write(json.dumps(title_tags))
                f.close()
                title_tags.clear()
                title_file_count += 1

            doc_count+=1
            if doc_count==30000:
                print("doc file count: ",file_count)
                sorted_dict = sorted(inverted_dict.items())
                filename = "inverted_index/" + str(file_count) + ".txt"
                write_in_file = ""
                for entry in sorted_dict:
                    write_in_file += (str(entry) + '\n')
                with open(filename,'w') as f:
                    f.write(write_in_file)
                f.close()
                inverted_dict.clear()
                keyword_set.clear()
                doc_words.clear()
                cache_stemming.clear()
                sorted_dict.clear()
                doc_count=0
                file_count+=1
        
        elif tname == "text" and elem.text != None:
            text = (elem.text).lower()
            # finding category
            category_list = re.findall('\[\[category:([^\n]+)\]\]', text)   #this have very specific patterns   
            for string in category_list:
                process_text(string,category_words)
                
            # finding infobox
            infobox_list = re.findall('\{\{infobox([\w\W]+)\}\}', text)    #this have very specific pattern
            for string in infobox_list:
                process_text(string,infobox_words)
                
            # finding links
            links_list = re.findall("==external links==([\w\W]+)\}\}", text)  #pattern is to find the initial written line of external link and ending with }}
            for string in links_list:
                process_text(string,links_words)
                
            # finding references
            references_list = re.findall('\* \[([\w\W]+)\{\{', text)       #pattern is "* [..]"
            for string in references_list:
                process_text(string,references_words)
                
            # finding body words  ---> Very loosely handled
            process_text(text,body_words)
            
            # adding out keywords in the inverted list
            add_to_the_list(title_words,body_words,infobox_words,category_words,references_words,links_words,id)
            
            title_words.clear()
            doc_words.clear()
            body_words.clear()
            infobox_words.clear()
            category_words.clear()
            references_words.clear()
            links_words.clear()
        
            
    elem.clear()


if t_cnt != 0:
    # writing the title tag and their doc ids in a file
    filename = "./final_inverted_index/title/" + str(title_file_count) + ".txt"
    with open(filename,'w') as f:
        f.write(json.dumps(title_tags))
    f.close()
    title_tags.clear()

with open("./final_inverted_index/title/title_helper.txt",'w') as f:
    f.write(json.dumps(title_ids))
f.close()
title_ids.clear()

# if at last doc count remain less than 10000
if doc_count != 0:
    sorted_dict = sorted(inverted_dict.items())
    filename = "inverted_index/" + str(file_count) + ".txt"
    write_in_file = ""
    for entry in sorted_dict:
        write_in_file += (str(entry) + '\n')
    with open(filename,'w') as f:
        f.write(write_in_file)
    f.close()
    inverted_dict.clear()
    keyword_set.clear()
    doc_words.clear()
    sorted_dict.clear()
    cache_stemming.clear()
      
time_took = time.time() - start_time
print(hms_string(time_took))
