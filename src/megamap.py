from collections import Counter
from collections import defaultdict
import string
from english_stoplist import stoplist
import cloud
import os
import time

### don't forget to set your own API key and secret in cloud_config.py
from cloud_config import *
cloud.setkey(key,secret)

def cloud_status(jids):
    s=Counter(cloud.status(jids))
    return s   

def inner_map(x): return x*x

def outer_map(y):
    jids=cloud.map(inner_map,range(y))
    
    cloud.join(jids)
    
    results = cloud.result(jids)
    
    return list(results)
    

jids=cloud.map(outer_map,range(5))
cloud.join(jids)
results = cloud.result(jids)