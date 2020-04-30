import os
import pickle
from math import floor, log2
# from pprint import pprint


def constructGraph():
    fileName = os.getcwd() + '\\Data\\wt2g_inlinks.txt'

    inlinks = {}  # {url: [inlink1, inlink2, ...], ...}
    outlinks = {}  # {url: count, ...}
    # x = 0

    print('constructing graph')

    with open(fileName, 'r') as wt2g:
        line = wt2g.readline()

        while line:
            splitLine = line.strip().split(sep = ' ')

            current = splitLine[0]  # get the current page

            if current not in inlinks:  # new page
                inlinks[current] = set(splitLine[1 :])  # unique inlinks of new current page

            # else:  # add new inlinks, commented out as x is 0
            #     inlinks[current].extend(splitLine[1 :])
            #     x += 1

            # get outlinks
            if current not in outlinks:  # ensures that all pages are in outlinks
                outlinks[current] = 0

            for page in set(splitLine[1 :]):  # set removes duplicates
                outlinks[page] = outlinks[page] + 1 if page in outlinks else 1  # count outlinks of all pages
            
            line = wt2g.readline()

    return inlinks, outlinks

def perplexity(pagerank):
    h = 0
    
    for p in pagerank:
        h += pagerank[p] * log2(pagerank[p])  # entropy

    return 2 ** (-1 * h)  # perplexity

def isConverged(pplxty):  # wt2g converges between 55 and 60 iterations
    pxy = [floor(i) % 10 for i in pplxty]  # round down and extract last digit
    return all(i == pxy[0] for i in pxy) and len(pxy) >= 4  # len condition prevents convergence before 4 iterations

def readPckl(file):
    obj = None

    with open(file, 'rb') as pckl:
        obj = pickle.load(pckl)

    pckl.close()

    return obj

# def getOutlinks():
#     fileName = os.getcwd() + '\\allInlinks.pickle'

#     inlinks = readPckl(fileName)
#     outlinks = {i: 0 for i in inlinks}

#     for page in inlinks:
#         # if page not in outlinks:
#         #     outlinks[page] = 0

#         for inl in inlinks[page]:
#             if inl in outlinks:
#                 outlinks[inl] += 1

#     return inlinks, outlinks

def calcPR(fileMode = True):
    inlinks, outlinks = None, None
    opFile = 'wt2g_res.txt'

    if fileMode:
        inlinks, outlinks = constructGraph()

    # else:  
    #     inlinks, outlinks = getOutlinks()
    #     opFile = 'crawlRes_ALL.txt'

    count = 0  # iterations
    d = 0.85  # teleportation factor
    N = len(inlinks)

    sinks = set([i for i in outlinks if outlinks[i] == 0])  # pages without outlinks

    pagerank = {}
    pplxty = []  # perplexity values

    for p in inlinks:
        pagerank[p] = 1 / N  # initial pagerank: equal prob for each page

    pplxty.append(perplexity(pagerank))  # initial perplexity
    
    while not isConverged(pplxty[-4 :]):  # check last 4 values for convergence
        count += 1
        print(count)

        npr = {}  # new page ranks
        sinkPR = 0  # total sink prob

        for s in sinks:
            sinkPR += pagerank[s]  # calculate total sink prob

        for p in inlinks:  # get new pagerank for each page
            npr[p] = ((1 - d) / N) + ((d * sinkPR) / N)

            for inl in inlinks[p]:
                if inl in pagerank:
                    npr[p] += (d * pagerank[inl]) / outlinks[inl]

        for p in inlinks:  # update pageranks
            pagerank[p] = npr[p]

        pplxty.append(perplexity(pagerank))

    sortedPR = sorted(pagerank.items(), key = lambda x: (x[1], x[0]), reverse = True)[: 500]  # top 500 by pagerank

    # print('Rank   Page          PageRank             Outlinks   Inlinks')
    # for i in range(10):  # print top 10
    #     print(i + 1, '  ', sortedPR[i][0], ' ', sortedPR[i][1], ' ', outlinks[sortedPR[i][0]], '    ', len(inlinks[sortedPR[i][0]]))

    writeToFile(sortedPR, inlinks, outlinks, opFile)

def writeToFile(sortedPR, inlinks, outlinks, opFile):
    f = open(os.getcwd() + '\\Results\\' + opFile, 'w')

    for i in range(len(sortedPR)):
        f.write(str(i + 1) + ' ' + sortedPR[i][0] + ' ' + str(sortedPR[i][1]) + ' ' + str(outlinks[sortedPR[i][0]]) + ' ' + str(len(inlinks[sortedPR[i][0]])) + '\n')

    f.close()

calcPR()