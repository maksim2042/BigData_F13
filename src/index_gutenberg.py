from collections import Counter
from collections import defaultdict
import string
from english_stoplist import stoplist
import cloud
import os
import time
import requests
from StringIO import StringIO

from gutenberg import urls

### don't forget to set your own API key and secret in cloud_config.py
from cloud_config import *
cloud.setkey(key,secret)

def cloud_status(jids):
    s=Counter(cloud.status(jids))
    return s   


def url_chunker(url, chunksize=1024):
    """Returns an iterator over contents of a file
        *Params*
        #file - an open FILE object
        #chunksize - how many lines to read at once?
    """
    #url=book[0]
    #bookname=book[1]
    
    user_agent = {'User-agent': 'Mozilla/5.0'}
    result=requests.get(url,headers=user_agent)
    
    try:
        doc = result.content
    except:
        raise Exception("URL "+url+"not responding")
    
    text_in=StringIO(doc)
    chunks = []
    stop = False
    while not stop:
        text=""
        for x in range(chunksize):
            try:
                text+=text_in.next()
            except StopIteration:
                chunks.append(text)
                stop=True
                break
                
        chunks.append(text)
        
    jobids = cloud.map(wordcount, [(url,c) for c in chunks])
    cloud.join(jobids)
    results = cloud.result(jobids)
    
    index=reduce_results(results)
    return index

def wordcount(chunk):
    """
    The MAP part of Map/Reduce
    
    Produces a Counter() object of word counts in a given chunk of text 
    """
    filename= chunk[0]
    text = chunk[1]
    wordcount=Counter()
    for line in text.split('\n'):
        line=line.lower().strip()
        out = line.translate(string.maketrans("",""), string.punctuation)
        words = [w for w in out.split(' ') if w not in stoplist]
        wordcount.update(words)
    del wordcount['']
    return filename,wordcount

def reduce_results(results):
    """
    The REDUCE side of Map/Reduce
    
    ==PARAMS==
    results: A list of Counter() objects, as produced by cloud.result() method
    
    ==RETURNS==
    a Counter() object with total word-counts for the whole body of text
    """
    
    index = defaultdict(Counter)
    for f,r in results:
        for word,count in r.iteritems():        
            index[word].update({f:count})
    return index


def super_reducer(results):
    """
    ### results is a LIST of Indexes where each index is :
    ### word: Counter(filename:number)
    
    ### returns index in shape of word: Counter(filename:number)
    """
    superindex=defaultdict(Counter)
    for index in results:
        for word in index:
            superindex[word].update(index[word])

    return superindex

def gen_gutenberg_url(number):
    base_url = "http://www.gutenberg.org/cache/epub/$NUM/pg$NUM.txt"
    return(base_url.replace('$NUM',number))


def mongo_insert(index):
    """
    Inserts indexes into MongoDB
    """
    conn=pymongo.connection.MongoClient(host='localhost')
    db=conn.test
    for word,i in index.iteritems():
        print word, i
        try:
            db.shakespeare.save({'word':unicode(word),'index':list(i.iteritems())})
        except UnicodeDecodeError:
            print(":-P")
            pass

def search(text, numresults=10):
    """
    Search against the index in MongoDB and return a list of URLs 
    """
    words = text.split()
    conn=pymongo.connection.MongoClient(host='localhost')
    db=conn.test
    
    result=Counter()
    for word in words:
        record = db.shakespeare.find_one({'word':word})
        if record and 'index' in record:
            links = record['index']
            print links
            result.update(dict(links))
    
    return result.most_common(numresults)



#fixed_urls = [(gen_gutenberg_url(x),y) for x,y in urls]
from shakespeare import shakespeare
urls = ['http://www.gutenberg.lib.md.us/etext00/0'+u+'.txt' for u in shakespeare]

jids=cloud.map(url_chunker,urls)
cloud.join(jids)
res=cloud.result(job_ids)
index=super_reducer(res)
mongo_inser(index)