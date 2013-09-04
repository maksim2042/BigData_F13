from collections import Counter
import string
from english_stoplist import stoplist
import cloud
import os
import time

### don't forget to set your own API key and secret in cloud_config.py
from cloud_config import *
cloud.setkey(key,secret)

#filename = "../data/hamlet.txt"
#filename = "../data/bacon_the_advancement_of_learning.txt"
filename = "../data/sir_thomas_more.txt"
f = open(filename,'rb')

def cloud_status(jids):
    stati=cloud.status(jids)
    s=Counter()
    for st in stati: s[st]+=1
    return s   


def chunker(file,chunksize=1024):
    """Returns an iterator over contents of a file
        *Params*
        #file - an open FILE object
        #chunksize - how many lines to read at once?
    """
    while True:
        text=""
        for x in range(chunksize):
            try:
                text+=file.next()
            except StopIteration:
                return
                #yield text
        yield text

def filer(path):
    """returns an iterator over a list of files (all files in <path>)
       each item in the iterator is a FILE object (i.e. still needs to be read)
       
       ==Params==
       path: a valid file-system path
    """
    for f in os.listdir(path):
        yield open(path+'/'+f,'rb')
        
def filechunker(path):
    """
        returns an iterator over chunks of files, for all files in a given path
        
        ==Params==
        path: a valid file-system path
    """
    for f in filer(path):
        for chunk in chunker(f):
            yield chunk


def wordcount(text):
    """
    The MAP part of Map/Reduce
    
    Produces a Counter() object of word counts in a given chunk of text 
    """
    wordcount=Counter()
    for line in text.split('\n'):
        line=line.lower().strip()
        out = line.translate(string.maketrans("",""), string.punctuation)
        words = [w for w in out.split(' ') if w not in stoplist]
        wordcount.update(words)
    del wordcount['']
    return wordcount


def reduce_results(results):
    """
    The REDUCE side of Map/Reduce
    
    ==PARAMS==
    results: A list of Counter() objects, as produced by cloud.result() method
    
    ==RETURNS==
    a Counter() object with total word-counts for the whole body of text
    """
    total_wordcount=Counter()
    for r in results:
        total_wordcount.update(r)
    return total_wordcount

##job_ids=cloud.map(wordcount,chunker(f))

## where are the files we care about?
path='../www.gutenberg.lib.md.us/etext00'

## start cloud jobs over chunks of text
job_ids=cloud.map(wordcount,filechunker(path))

while True:
    c=cloud_status(job_ids)
    print c
    if c['processing']==0:
        break
    else:
        sleep(10)

res=cloud.result(job_ids)











