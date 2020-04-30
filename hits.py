import elasticsearch
import numpy as np
import os
import pickle
from pprint import pprint
from random import sample

indexName = 'ww2_search'
esCFG = {'host': 'localhost', 'port': 9200}
es = elasticsearch.Elasticsearch(hosts = [esCFG], timeout = 600000)

def readPckl(file):
    obj = None

    with open(file, 'rb') as pckl:
        obj = pickle.load(pckl)

    pckl.close()

    return obj

def writePckl(file, obj):
    with open(file, 'wb') as pckl:
        pickle.dump(obj, pckl, pickle.HIGHEST_PROTOCOL)

    pckl.close()

def createBaseSet():
    d = 200
    base = {}
    root = {}

    body = {
        'from': 0,
        'size': 1000,
        'query': {
            'match': {
                'text': 'battle of stalingrad'
            }
        }
    }
    print('getting root')
    res = es.search(body = body, index = indexName)

    for doc in res['hits']['hits']:
        root[doc['_id']] = {'inl': doc['_source']['inlinks'], 'out': doc['_source']['outlinks']}

    base.update(root)

    count = 1
    for page in root:
        if len(base) <= 10000:
            print(count, len(base))

            for otl in base[page]['out']:
                if otl not in base:
                    try:
                        res = es.get(index = indexName, id = otl)
                        base[otl] = {'inl': res['_source']['inlinks'], 'out': res['_source']['outlinks']}
                    except:
                        pass

            inl = base[page]['inl']

            if len(inl) > d:  # at most 200 inlinks 
                inl = sample(inl, k = d)

            for i in inl:
                if i not in base:
                    try:
                        res = es.get(index = indexName, id = i)
                        base[i] = {'inl': res['_source']['inlinks'], 'out': res['_source']['outlinks']}
                    except:
                        pass

            count += 1

    writePckl(os.getcwd() + '\\Data\\base.pickle', base)
    writePckl(os.getcwd() + '\\Data\\root.pickle', root)

    hits(base)

def isConverged(prevAuth, authority, prevHub, hub):
    np_pa = np.array(list(prevAuth.values()))
    np_a = np.array(list(authority.values()))

    np_ph = np.array(list(prevHub.values()))
    np_h = np.array(list(hub.values()))

    a = np.allclose(np_pa, np_a, rtol = 0, atol = 1e-5)
    h = np.allclose(np_ph, np_h, rtol = 0, atol = 1e-5)

    return a and h

def hits(base, maxIter = 100):
    authority = {i: 1 for i in base}
    hub = {i: 1 for i in base}

    prevAuth, prevHub = {}, {}
    prevAuth.update(authority)
    prevHub.update(hub)

    print('hits')
    for i in range(maxIter):
        print(i)
        anorm, hnorm = 0, 0  # norms

        for page in base:
            authority[page] = 0
            inlinks = base[page]['inl']

            for inl in inlinks:  # inlinks of current page
                if inl in hub:
                    authority[page] += hub[inl]
            
            anorm += authority[page] ** 2

        anorm **= 0.5

        for page in base:
            hub[page] = 0
            outlinks = base[page]['out']

            for out in outlinks:  # outlinks of current page
                if out in authority:
                    hub[page] += authority[out]
                    
            hnorm += authority[page] ** 2

        hnorm **= 0.5

        for page in base:  # normalisaion
            authority[page] /= anorm
            hub[page] /= hnorm

        if isConverged(prevAuth, authority, prevHub, hub):
            print('Converged')
            break
        
        prevAuth.update(authority)
        prevHub.update(hub)

    sortedAuth = sorted(authority.items(), key = lambda x: (x[1], x[0]), reverse = True)[: 500]
    sortedHub = sorted(hub.items(), key = lambda x: (x[1], x[0]), reverse = True)[: 500]

    # pprint(sortedAuth)
    # pprint(sortedHub)

    writeToFile(sortedAuth, 'hits_auth.txt')
    writeToFile(sortedHub, 'hits_hub.txt')

    print('Done')

def writeToFile(sortedRes, opFile):
    f = open(os.getcwd() + '\\Results\\' + opFile, 'w')

    for i in range(len(sortedRes)):
        f.write(sortedRes[i][0] + ' ' + str(sortedRes[i][1]) + '\n')

    f.close()

def main(usePckl = False):
    base, root = None, None

    if usePckl:
        base = readPckl(os.getcwd() + '\\Data\\base.pickle')
        hits(base)
    else:
        base = createBaseSet()

main()