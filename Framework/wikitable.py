#!/usr/bin/env python2
# -*- coding: utf-8 -*-
from bs4 import BeautifulSoup
import urllib2
import pymongo
from restclient import GET
import urllib
import simplejson as json
import time
import random
import zipfile
import base64
import zlib
from StringIO import StringIO
#import freebase
from apiclient import discovery, model
import freebase
import string 
import re
import distance 
import operator
import cPickle as pickle
import time

REL_DOCS_LEN = "path to rel docs len"

def parseTable(target):
    results_list = []
    print target['query_id'], target['wiki_page']
    #target = {'wiki_page':tsv[3], 'query_id':tsv[4], 'table': tsv[7], 'columns': tsv[9].strip(' ').strip(';').split(';') }

    header = {'User-Agent': 'Mozilla/5.0'} #Needed to prevent 403 error on Wikipedia
    req = urllib2.Request(target['wiki_page'],headers=header)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)
    title = ""
    director = ""
    cast = ""
    country = ""
    results = soup.findAll("table", { "class" : "wikitable" })
    f = open('output.csv', 'w')
    for table in results:
        theader = 0
        columns_list = []
        for row in table.findAll("tr"):
            if theader == 0:
                columns = row.findAll(['th', 'td'])
                if columns:
                    for each in columns:
                        columns_list.append(each.find(text=True))
                    print 'columnst in table.......', columns_list
                    theader = 1
                    continue
            if theader == 1:
                isTargetTable = True
                indexes = []
                for column in target['columns']:
                    if column not in columns_list:
                        isTargetTable = False
                        #print column, '-------', columns_list
                        break
                    else:
                        indexes.append(columns_list.index(column))

                if isTargetTable == True:
                    cells = row.findAll(['th','td'])

                    #For each "tr", assign each "td" to a variable.
                    if len(cells) == len(columns_list):
                        items = []
                        for index in indexes:
                            link = cells[index].find('a')
                            if link:
                                #print link['href'].replace('/wiki/','')
                                text = link.find(text=True)
                                if text != '':
                                    items.append(text)
                            else:
                                break
                            if len(items) == 2:
                                if items not in results_list:
                                    results_list.append(items)
                    else:
                        #print 'ERROR:: len of columns in rows is different from header'
                        bla = 0
    print len(results_list)
    db = pymongo.MongoClient('localhost:27123').structured
    target['results_wiki'] = results_list
    db.natasa_judgements.insert(target)


def mapEntities():
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    for table in db.st2.find({'relational':True}):
        for row in table['content']:
            for column in row:
                for entity in row[column]:
                    print entity
                    try:
                        db.sample_entities.insert({'_id':entity})
                    except Exception,e:
                        print str(e)


def parseDbListofLists(start_level):
    db = pymongo.MongoClient('localhost:27123').structured
    all_hrefs = db.wiki_lists.distinct('href')
    levels_dict = {}
    levels_dict[start_level] = {}
    for page in db.wiki_lists.find({'level':start_level}):
        levels_dict[start_level][page['href']] = {'hasTable': page['hasTable'], 'childs': page['childs']}
    for level in xrange(start_level, start_level+20):
        print level
        levels_dict, all_hrefs = searchLevel(level, levels_dict, all_hrefs)
        if level != start_level:
            for page in levels_dict[level].keys():
                doclist = {'href': page, 'level': level, 'hasTable':levels_dict[level][page]['hasTable'], 'childs': levels_dict[level][page]['childs']}
                db.wiki_lists.insert(doclist)



def parseListofLists(root_url):
    #for each level get the number of lists with tables
    #if childs get tables
    db = pymongo.MongoClient('localhost:21213').structured
    soup = getSoup(root_url)
    results = soup.findAll('a', href=True)
    levels_dict = {}
    all_hrefs = []
    level_one = []
    level = 1
    levels_dict[level] = {} 
    for each in results:
        if each['href'].startswith('/wiki/List'):
            levels_dict[level][each['href']]= {'hasTable':None, 'childs':[]}
            all_hrefs.append(each['href'])
            #print each['href']
    for level in xrange(1,10):
        print level
        levels_dict, all_hrefs = searchLevel(level, levels_dict, all_hrefs)
        for page in levels_dict[level].keys():
            doclist = {'href': page, 'level': level, 'hasTable':levels_dict[level][page]['hasTable'], 'childs': levels_dict[level][page]['childs']}
            db.wiki_lists_2016.insert(doclist)

def getSoup(url):
    soup = None
    try:
        header = {'User-Agent': 'Mozilla/5.0'} #Needed to prevent 403 error on Wikipedia
        req = urllib2.Request(url,headers=header)
        page = urllib2.urlopen(req)
        soup = BeautifulSoup(page)
    except Exception,e:
        print str(e)
    return soup

def parseListofLists(last_level): # method similar to parseListofLists(root_url) but starts by reading all lists in a given level in mongodb instead of root_utl
    #for each level get the number of lists with tables
    #if childs get tables
    db = pymongo.MongoClient('localhost:21213').structured
    levels_dict = {}
    all_hrefs = []

    level = last_level
    levels_dict[level] = {} 
    for page in db.wiki_lists_2016.find({'level':last_level}):
        for href in page['childs']:
            levels_dict[level][href] = {'hasTable':None, 'childs':[]}
    for level in xrange(level,10):
        print level
        levels_dict, all_hrefs = searchLevel(level, levels_dict, all_hrefs)
        for page in levels_dict[level].keys():
            doclist = {'href': page, 'level': level, 'hasTable':levels_dict[level][page]['hasTable'], 'childs': levels_dict[level][page]['childs']}
            db.wiki_lists_2016.insert(doclist)

def getSoup(url):
    soup = None
    try:
        header = {'User-Agent': 'Mozilla/5.0'} #Needed to prevent 403 error on Wikipedia
        req = urllib2.Request(url,headers=header)
        page = urllib2.urlopen(req)
        soup = BeautifulSoup(page)
    except Exception,e:
        print str(e)
    return soup



def searchLevel(level, tree_dict, all_hrefs):
    print 'searchLevel', level
    tree = tree_dict
    next_level = level + 1
    count = 0
    for href in tree[level].keys():
        count +=1
        print "level, link_number, href ", level, count, href
        soup = getSoup('http://en.wikipedia.org' + href)
        if soup:
            has_table = checkTables(soup)
            tree[level][href]['hasTable'] = has_table
            if not has_table:
                childs, all_hrefs = getListPages(soup, all_hrefs)
                if childs:
                    tree[level][href]['childs'] = childs
                    #print href, tree[level][href]
                    if next_level not in tree.keys():
                        tree[next_level] = {}
                    for each in childs:
                        tree[next_level][each] = {'hasTable':None, 'childs':[]}
    return tree, all_hrefs



def getListPages(soup, all_hrefs):
    results = soup.findAll('a', href=True)
    lists_list = []
    for each in results:
        if each['href'].startswith('/wiki/List') and each['href'] not in all_hrefs:
            lists_list.append(each['href'])
            all_hrefs.append(each['href'])
    return lists_list, all_hrefs


def checkTables(soup):
    results = soup.findAll('table', class_=re.compile('wikitable'))
    #results = soup.findAll("table", { "class" : "wikitable sortable" })
    if results:
        return True
    else:
        return False

def getStats():  
    db = pymongo.MongoClient('localhost:27123').structured
    lists_num = db.wiki_lists.find().count()
    tables_num = db.wiki_lists.find({'hasTable':True}).count()
    print 'level, pages, pages %, hasTable, hasTable %'
    for level in xrange(0,34):
        pages = db.wiki_lists.find({'level':level}).count()
        hasTable = db.wiki_lists.find({'level':level, 'hasTable':True}).count()
        print level, ',', pages,',',"{0:.2f}".format(float(pages)/float(lists_num) *100) , ',',  hasTable, ',', "{0:.2f}".format(float(hasTable)/float(tables_num) *100)


def readTsv(filepath):
    targets_list = []
    for line in open(filepath):
        tsv = line.strip('\n').split('\t')
            #print tsv[3], '-------', tsv[4],  '-------' , tsv[7],'-------' , tsv[9]
        target = {'wiki_page':tsv[1], 'query_id':tsv[2], 'table': tsv[4], 'columns': tsv[6].strip(' ').strip(';').split(';') }
        if len(target['columns']) == 2:
            targets_list.append(target)
    return targets_list

def getListTuples(target):
    return None


def getJudgements():
    filepath = '/home/popstar/Documents/StructuredSearchQueries-2015-PilotSample-Manual.csv'
    target_list = readTsv(filepath)
    for target in target_list:
        print '__________________'
        print target['columns']
        parseTable(target)



def getWikifromSample():
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    count = 1
    for table in db.st2.find():
        print count
        count += 1
        for row in table['content']:
            for column in table['relevant_columns']:
                for entity in row[column]:
                    db.sample_entities.update({'_id':entity},{'$set':{'sample':'all'}}, upsert=True)


def getWikiToFreebase():
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    maps = {}
    count = 1
    for line in open('/media/popstar/Novo volume/Freebase/mid2wikipedia.tsv'):
        info = line.strip('\n').split('\t')
        maps[info[2]] = info[0]
        print count
        count += 1
    for entity in db.sample_entities.find({'mid':{'$exists':0}}):
        #if '%' in key:
        #    key = urllib.unquote(key.encode('utf-8'))
        try:
            key = entity['_id'].replace('/wiki/','')
            mid = maps[key]
            #mid=maps[str(key)]
            print mid
            if mid:
                db.sample_entities.update({'_id':entity['_id']},{'$set':{'mid':mid}})
        except Exception, e:
            print str(e)


def getRootList():
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    count = 1
    for table in db.st2.find({'qrels':{'$exists':1}}):
        print count
        count+=1
        url = table['url'].replace('http://en.wikipedia.org','')
        root_url, level = getRoot(url)
        try:
            category = db.list_categories.find_one({'childs':root_url})['category']
            root_url = 'http://en.wikipedia.org'+ root_url
            db.st2.update({'_id':table['_id']},{'$set':{'root_url':root_url, 'level':level, 'category':category}})
        except Exception,e:
            continue

def getRoot(url):
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    db2 = pymongo.MongoClient('popstar.fe.up.pt:21213').structured
    level = db2.wiki_lists_2016.find_one({'href':url})['level']
    father = db2.wiki_lists_2016.find_one({'childs':url})
    if father:
        while (father['level'] != 1):
            print father['href'], father['level']
            father = db2.wiki_lists_2016.find_one({'childs':father['href']})
    else:
        father = db2.wiki_lists_2016.find_one({'href':url, 'level':1})
    if father:
        print 'Return..........', father['href']
        return father['href'], level
    else:
        return 'error'

def getWikiLink():
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    for table in db.st2.find({'relational':True}):
        print table['url']
        count_entities = 0
        count_mids = 0
        for row in table['content']:
            for column in table['relevant_columns']:
                for entity in row[column]:
                    count_entities += 1
                    mid = db.sample_entities.find_one({'_id':entity,'mid':{'$exists':1}})
                    if mid:
                        if 'mid' in mid.keys() and mid['mid']:
                            if mid['mid'].startswith('/m/'):
                                count_mids += 1
                        else:
                            print 'MID not in keys.-------------------------------------------------------------------------------------------------'
        mids_resolved = False
        if count_entities == count_mids:
            mids_resolved = True
        db.st2.update({'_id':table['_id']},{'$set':{'entities_num':count_entities, 'mids_num':count_mids, 'mids_resolved':mids_resolved}})



def getKeyColumn():
    print "getKeyColumn"
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    count = 0
    for table in db.st2.find({'relational':{'$exists':0}}):
        print count
        if len(table['table_title'].replace('[edit]','')) > 2:
            kcolumn = None
            count_entities = 0
            key_dict = {}
            key_rat = {}
            for row in table['content']:
                for column in row:
                    for entity in row[column]:
                        if not entity.startswith('/w/index.php'):
                            try:
                                key_dict[column].append(entity)
                            except KeyError:
                                key_dict[column] = [entity]
            for key in key_dict:
                if len(key_dict[key]) > 4:
                    key_rat[key] = float(len(set(key_dict[key]))) / float(len(key_dict[key]))
                #print key#, key_dict[k]
            print '_________________________________________'
            print table['url']
            print table['columns']
            print table['table_title']
            if len(key_rat) < 2:
                relational = False
            else:
                
                sorted_ = sorted(key_rat.items(), key=operator.itemgetter(1), reverse=True)
                print sorted_
                maxscore = sorted_[0][1]
                kcolumn = sorted_[0][0]
                index = table['columns'].index(sorted_[0][0])
                for i in xrange(len(sorted_)):
                    if i > 0:
                        if (maxscore - sorted_[i][1]) <= 0.1 :
                            index2 =  table['columns'].index(sorted_[i][0])
                            if index2 < index:
                                kcolumn = sorted_[i][0]
                                index = index2

                print 'KEY COLUMN: ', kcolumn
            #create Relational:True
            if kcolumn == None:
                relational = False
                db.st2.update({'_id':table['_id']},{'$set':{'relational':False}})
            else:
                pairs = []

                for key in key_rat:
                    if key != kcolumn:
                        pairs.append((kcolumn,key))

                print pairs
                db.st2.update({'_id':table['_id']},{'$set':{'relational':True,'Key_column':kcolumn,'relations':pairs}})


def createEntityPairs():
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    count = 0
    """
    for table in db.st2.find({'relational':True}):
        count += 1
        print count
        relations_content = {}
        triples = []
        if len(table['relations']) > 1:
            for i in xrange(len(table['relations'])-1):
                triples.append((table['relations'][i][0], table['relations'][i][1], table['relations'][i+1][1]))
            print triples
            db.st2.update({'_id':table['_id']},{'$set':{'triples':triples}})
    """
    for table in db.st2.find({'relational':True}):
        for relation in table['relations']:
            mids_rows = []
            for row in table['content']:
                if relation[0] in row and relation[1] in row:
                    mids = None
                    mid1 = db.sample_entities.find_one({'_id':row[relation[0]][0]})
                    mid2 = db.sample_entities.find_one({'_id':row[relation[1]][0]})
                    try:
                        mid1 = mid1['mid']
                        mid2 = mid2['mid']
                        if mid1 < mid2:
                            mids = mid1+'#'+mid2
                        else:
                            mids = mid2+'#'+mid1
                        mids_rows.append(mids)
                    except Exception,e:
                        continue
            if len(mids_rows) > 0:
                key = '###'.join(relation)
                relations_content[key] = mids_rows
        try:
            if relations_content:
                max_pairs = 0
                key_relation = ''
                for key in relations_content:
                    mids_num = len(relations_content[key])
                    if mids_num > max_pairs:
                        max_pairs = mids_num
                        key_relation = key
                print max_pairs, key_relation
                db.st2.update({'_id':table['_id']},{'$set':{'relations_content': relations_content, 'max_mids':max_pairs, 'key_relation':key_relation}})
        except Exception,e:
            print str(e)

def createEntityPairs():
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    count = 0
    """
    for table in db.st2.find({'relational':True}):
        count += 1
        print count
        relations_content = {}
        triples = []
        if len(table['relations']) > 1:
            for i in xrange(len(table['relations'])-1):
                triples.append((table['relations'][i][0], table['relations'][i][1], table['relations'][i+1][1]))
            print triples
            db.st2.update({'_id':table['_id']},{'$set':{'triples':triples}})
    """
    for table in db.st2.find({'relational':True}):
        for relation in table['relations']:
            mids_rows = []
            for row in table['content']:
                if relation[0] in row and relation[1] in row:
                    mids = None
                    mid1 = db.sample_entities.find_one({'_id':row[relation[0]][0]})
                    mid2 = db.sample_entities.find_one({'_id':row[relation[1]][0]})
                    try:
                        mid1 = mid1['mid']
                        mid2 = mid2['mid']
                        if mid1 < mid2:
                            mids = mid1+'#'+mid2
                        else:
                            mids = mid2+'#'+mid1
                        mids_rows.append(mids)
                    except Exception,e:
                        continue
            if len(mids_rows) > 0:
                key = '###'.join(relation)
                relations_content[key] = mids_rows
        try:
            if relations_content:
                max_pairs = 0
                key_relation = ''
                for key in relations_content:
                    mids_num = len(relations_content[key])
                    if mids_num > max_pairs:
                        max_pairs = mids_num
                        key_relation = key
                print max_pairs, key_relation
                db.st2.update({'_id':table['_id']},{'$set':{'relations_content': relations_content, 'max_mids':max_pairs, 'key_relation':key_relation}})
        except Exception,e:
            print str(e)


def createEntityTriples():
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    count = 0

    for table in db.st2.find({'triples':{'$exists':1}}):
        for relation in table['relations']:
            mids_rows = []
            for row in table['content']:
                if relation[0] in row and relation[1] in row:
                    mids = None
                    mid1 = db.sample_entities.find_one({'_id':row[relation[0]][0]})
                    mid2 = db.sample_entities.find_one({'_id':row[relation[1]][0]})
                    try:
                        mid1 = mid1['mid']
                        mid2 = mid2['mid']
                        if mid1 < mid2:
                            mids = mid1+'#'+mid2
                        else:
                            mids = mid2+'#'+mid1
                        mids_rows.append(mids)
                    except Exception,e:
                        continue
            if len(mids_rows) > 0:
                key = '###'.join(relation)
                relations_content[key] = mids_rows
        try:
            if relations_content:
                max_pairs = 0
                key_relation = ''
                for key in relations_content:
                    mids_num = len(relations_content[key])
                    if mids_num > max_pairs:
                        max_pairs = mids_num
                        key_relation = key
                print max_pairs, key_relation
                db.st2.update({'_id':table['_id']},{'$set':{'relations_content': relations_content, 'max_mids':max_pairs, 'key_relation':key_relation}})
        except Exception,e:
            print str(e)


def getCoverage():
    with open(REL_DOCS_LEN, 'rb') as handle:
        rel_docs_len = pickle.load(handle)
    print 'ClueWeb relations loaded!'
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    count = 0
    for table in db.st2.find({'relations_content':{'$exists':1}}):
        count += 1
        print count
        qrels_dict = {}
        max_coverage = 0
        for relation in table['relations_content']:
            coverage = 0
            qrels = []
            for pair in table['relations_content'][relation]:
                if pair in rel_docs_len:
                    coverage += 1
                    qrels.append(pair)
            if len(qrels) > 0:
                qrels_dict[relation] = qrels
            if coverage > max_coverage:
                max_coverage = coverage
        db.st2.update({'_id':table['_id']},{'$set':{'qrels': qrels_dict, 'max_coverage':max_coverage}})


def searchGroups(mids_pair_list):
    results = {}
    db2 = pymongo.MongoClient('popstar.fe.up.pt:21213').structured
    db3 = pymongo.MongoClient('popstar.fe.up.pt:21212').structured
    for relation in mids_pair_list.keys():
        results[relation] = []
        for each in db2.wiki_relations_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
        for each in db2.relations_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
        for each in db2.relations_0001_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
        for each in db2.relations_0011_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
        for each in db2.relations_0008_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
        for each in db2.relations_0009_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
        for each in db2.relations_0010_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
        for each in db3.relations_0004_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
        for each in db3.relations_0007_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
        for each in db3.relations_0006_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
        for each in db3.relations_0005_group.find({'_id':{'$in':mids_pair_list[relation]}}):
            if each['_id'] not in results[relation]:
                results[relation].append(each['_id'])
    return results




def getCurID(url):
#wgArticleId
    header = {'User-Agent': 'Mozilla/5.0'} #Needed to prevent 403 error on Wikipedia
    req = urllib2.Request(url,headers=header)
    page = urllib2.urlopen(req)
    #3936054,"wgIsArticle"
    soup = BeautifulSoup(page)
    source=soup.findAll('script',{"src":True})
    for script in soup.find_all('script'):
        result = re.search('\"wgArticleId\":(.*),\"wgIsArticle\"', script.text)
        if result:
            curid = result.group(1)
            return curid
    return None


def getTopicText(filename): 
    db = pymongo.MongoClient('localhost:27543').wikitables2017
    urls = []
    for each in db.st2.find({}):
        urls.append(each['url'])
    for line in open(filename):
        try:
            lj = json.loads(line.strip('\n'))
            if lj['fetch_url'] in urls:
                if 'fetch_data' in lj.keys():
                    soup = BeautifulSoup(lj['fetch_data'])
                    page_topic = soup.findAll('p')
                    if page_topic:
                        print page_topic[0].text
                        db.st2.update({'url':lj['fetch_url']},{'$set':{'page_topic':page_topic[0].text}}) 
        except Exception,e:
            print str(e)
    
    
    
def quotekey(ustr):
    """
    quote a unicode string to turn it into a valid namespace key
    """
    valid_always = string.ascii_letters + string.digits
    valid_interior_only = valid_always + '_-'
    if isinstance(ustr, str):
        s = unicode(ustr,'utf-8')        
    elif isinstance(ustr, unicode):
        s = ustr
    else:
        raise ValueError, 'quotekey() expects utf-8 string or unicode'
    output = []
    if s[0] in valid_always:
        output.append(s[0])
    else:
        output.append('$%04X' % ord(s[0]))
    for c in s[1:-1]:
        if c in valid_interior_only:
            output.append(c)
        else:
            output.append('$%04X' % ord(c))
    if len(s) > 1:
        if s[-1] in valid_always:
            output.append(s[-1])
        else:
            output.append('$%04X' % ord(s[-1]))
    return str(''.join(output))

"""
def getCoverage():
    db = pymongo.MongoClient('localhost:27123').structured
    for query in db.natasa_judgements.find():
        print query['query_id']
        count = 0

        for pair in query['mids_list']:
            res = db.wiki_relations_doc.find_one({'mids':pair})
            if res:
                count += 1
        if len(query['mids_list']) > 0:
            print len(query['mids_list']), count, float(count) / float(len(query['mids_list']))
"""


def testSample():
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    out = open('WIKIREL-2_PAIRS.tsv','wb')
    out.write('Query_ID\tURL\tPage Title\tTable Title\tRelation\tNL Query\tqA\tqR\tqB\n')
    topic = 0
    
    for table in db.tables_randompairs.find({'triples':{'$exists':0}}):
        topic += 1
        if not 'page_topic' in table.keys():
            table['page_topic'] = 'None '
        topic_id = 'WIKIREL-2_P_'
        if topic > 99:
            topic_id += '0' + str(topic)
        elif topic > 9:
            topic_id += '00' + str(topic)
        else:
            topic_id += '000' + str(topic)
        query = 0
        for key in table['qrels']:
            query += 1
            query_id = topic_id + '_0' + str(query)
            out.write(query_id.encode('utf-8')+ '\t' + '=HYPERLINK(\"' + table['url'].encode('utf-8') +'\")\t'  +  table['page_title'].encode('utf-8') + '\t'  + table['table_title'].encode('utf-8') + '\t' + key.encode('utf-8') +  '\t  ' + '\t  '+ '\t  '+ '\t  ' + '\n')
    out.close()    

def testSampleT():
    db = pymongo.MongoClient('popstar.fe.up.pt:54321').wikitables2017
    out = open('WIKIREL-2_TRIPLES.tsv','wb')
    out.write('Query_ID\tURL\tPage Title\tTable Title\tRelation\tNL Query\tqB\tqR1\tqA\tqR2\tqC\n')
    topic = 0
    
    for table in db.tables_randompairs.find({'triples':{'$exists':1}}):
        topic += 1
        if not 'page_topic' in table.keys():
            table['page_topic'] = 'None '
        topic_id = 'WIKIREL-2_T_'
        if topic > 99:
            topic_id += '0' + str(topic)
        elif topic > 9:
            topic_id += '00' + str(topic)
        else:
            topic_id += '000' + str(topic)
        query = 0

        query += 1
        query_id = topic_id + '_0' + str(query)
        key = table['triples'][0][1] + ' ### ' + table['triples'][0][0] + ' ### ' + table['triples'][0][2]
        out.write(query_id.encode('utf-8')+ '\t' + '=HYPERLINK(\"' + table['url'].encode('utf-8') +'\")\t'  +  table['page_title'].encode('utf-8') + '\t'  + table['table_title'].encode('utf-8') + '\t' + key.encode('utf-8') +  '\t  ' + '\t  '+ '\t  '+ '\t  ' + '\n')

    out.close()    




def checkRelevance(soup):
    results_list = []
    #target = {'wiki_page':tsv[3], 'query_id':tsv[4], 'table': tsv[7], 'columns': tsv[9].strip(' ').strip(';').split(';') }
    results = soup.findAll('table', class_=re.compile('wikitable'))
    f = open('output.csv', 'w')
    for table in results:
        theader = 0
        columns_list = []
        count_pairs = 0
        for row in table.findAll("tr"):
            if theader == 0:
                columns = row.findAll(['th', 'td'])
                if columns:
                    for each in columns:
                        columns_list.append(each.find(text=True))
                    #print 'columnst in table.......', columns_list
                    theader = 1
                    continue
            if theader == 1:
                cells = row.findAll(['th','td'])
                #For each "tr"vis., assign each "td" to a variable.
                if len(cells) == len(columns_list):
                    items = []
                    for cell in cells:
                        link = cell.find('a')
                        if link:
                            if not link['href'].startswith('/wiki/List_of'):
                                #print link['href']
                                if link['href'].startswith('/wiki/'):
                                    items.append(link['href'])
                            #text = link.find(text=True)
                            #if text != '':
                            #    items.append(text)
                    if len(items) > 1:
                        count_pairs += 1
            if count_pairs > 2:
                print 'At least 2 pairs'

                return count_pairs
                
    return False
    '''
    print len(results_list)
    db = pymongo.MongoClient('localhost:27123').structured
    target['results_wiki'] = results_list
    db.natasa_judgements.insert(target)
    '''

def checkAllpairs(soup, url):
    #print url
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    page_title = soup.find('h1').text
    page_topic = soup.findAll('p')
    if page_topic:
        #print page_topic[0].text
        page_topic = page_topic[0].text
    else:
        page_topic = ''
    table_list = soup.findAll('table', class_=re.compile('wikitable'))
    table_list_len = len(table_list)
    for table in table_list:
        table_doc = {}
        rows_list = []
        table_title = getTitle(table)
        if not table_title:
            table_title = page_title
        else:
            table_title = table_title.text
        table_doc['url'] = url
        table_doc['table_title'] = table_title
        table_doc['page_title'] = page_title
        table_doc['page_topic'] = page_topic
        table_doc['tables_num'] = table_list_len
        if table_title != 'References':
            rows_list, relevant_columns, columns_list = readRows(table)
            table_doc['columns'] = columns_list
            if rows_list and relevant_columns:
                rel_col_num = len(relevant_columns)
                if rel_col_num > 1:
                    print 'USEFUL....................', table_doc['url'], relevant_columns
                    table_doc['content'] = rows_list
                    table_doc['relevant_columns_num'] = rel_col_num
                    table_doc['relevant_columns'] = relevant_columns
                    #print "Relevant_Columns:", relevant_columns
                    try:
                        print 'updating.................'
                        db.st2.insert(table_doc)
                    except Exception,e:
                        print str(e)
    return False


def readRows(table):
    rows_list = []
    theader = 0
    columns_list = []
    count_rows = 0
    for row in table.findAll("tr"):
        row_dict = {}
        if theader == 0:
            columns = row.findAll(['th', 'td'])
            if columns:
                for each in columns:
                    column_name = each.find(text=True)
                    if column_name:
                        column_name = column_name.replace(',','')
                    else:
                        column_name = 'None'
                    columns_list.append(column_name)
                #print 'columns in table.......', columns_list
                theader = 1
                continue
        if theader == 1:
            count_rows += 1
            cells = row.findAll(['th','td'])
            #For each "tr", assign each "td" to a variable.
            if len(cells) == len(columns_list):
                for i in xrange(len(cells)):
                    links = cells[i].findAll('a')
                    #if len(links) == 1 and cells[i].text == links[0].text:
                    if len(links) == 1 and len(cells[i].text) < 100:
                        for link in links:
                            if not link['href'].startswith('/wiki/List_of') and not link['href'].startswith('/wiki/File:') and not link['href'].endswith('.svg') and not link['href'].endswith('.jpg'):
                                #print link['href']
                                if link['href'].startswith('/w'):
                                    try:
                                        row_dict[columns_list[i]].append(link['href'])
                                    except KeyError:
                                        row_dict[columns_list[i]] = [link['href']]

                if len(row_dict.keys()) > 1: #at least one pair with links
                    rows_list.append(row_dict)
            else:
                #print "::::::WARN::::: len(cells)!= len(header in table)"
                return None, None, None
    #alguma coisa correu mal aqui http://en.wikipedia.org/wiki/List_of_Latvians_in_the_NHL
    count_columns = {}
    relevant_columns = []
    for i in xrange(len(columns_list)):
        count_columns[columns_list[i]] = 0
    for row in rows_list:
        for key in row.keys():
            count_columns[key] += 1
    for i in xrange(len(columns_list)):
        #if count_columns[columns_list[i]] == count_rows:
        if count_columns[columns_list[i]] > min(5, count_rows):
            relevant_columns.append(columns_list[i])
    return rows_list, relevant_columns, columns_list




def bootstrap(soup):
    results_list = []
    #target = {'wiki_page':tsv[3], 'query_id':tsv[4], 'table': tsv[7], 'columns': tsv[9].strip(' ').strip(';').split(';') }
    results = soup.findAll("table", { "class" : "wikitable" })
    f = open('output.csv', 'w')
    for table in results:
        theader = 0
        columns_list = []
        relevant_columns = []
        count_pairs = 0
        for row in table.findAll("tr"):
            if theader == 0:
                columns = row.findAll(['th', 'td'])
                if columns:
                    for each in columns:
                        columns_list.append(each.find(text=True))
                    #print 'columnst in table.......', columns_list
                    theader = 1
                    continue
            if theader == 1:
                cells = row.findAll(['th','td'])
                #For each "tr", assign each "td" to a variable.
                if len(cells) == len(columns_list):
                    items = []
                    for i in xrange(len(cells)):
                        link = cells[i].find('a')
                        if link:
                            if not link['href'].startswith('/wiki/List_of'):
                                #print link['href']
                                if link['href'].startswith('/wiki/'):
                                    items.append(link['href'])
                                    relevant_columns.append(i)
                                    
                            #text = link.find(text=True)
                            #if text != '':
                            #    items.append(text)
                    if len(items) > 2:
                        count_pairs += 1
            if count_pairs > 2:
                print 'At least 2 pairs'
                print set(relevant_columns), columns_list
                return count_pairs
                
    return False
    
def setPairs(filename):
    count = 0
    for line in open(filename):
        lj = json.loads(line.strip('\n'))
        print lj['fetch_url']
        if 'fetch_data' in lj.keys():
            count += 1
            soup = BeautifulSoup(lj['fetch_data'])
            print count
            pairs = checkRelevance(soup)
            print pairs
            if pairs > 1:
                db = pymongo.MongoClient('localhost:27123').structured
                db.wiki_lists.update({'href':lj['fetch_url'].replace('http://en.wikipedia.org','')},  {'$set': {'hasPairs': True}})
            #print checkTables(soup)
            
    print count


#method that checks if there is a table in a wikipedia page with strict formatting and at least 2 columns 
#with all rows having hyperlinks to other wikipedia entities
def setAllpairs(filename):
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    count = 0
    for line in open(filename):
        count += 1
        print count
        try:
            processed = db.st2.distinct('url')
            lj = json.loads(line.strip('\n'))
            if lj['fetch_url'] not in processed:
                if 'fetch_data' in lj.keys():
                    soup = BeautifulSoup(lj['fetch_data'])
                    checkAllpairs(soup, lj['fetch_url'])
        except Exception,e:
            print 'setAllpairs', str(e)
    print count


    

def getSampleFureteur(filename):
    sample = []
    for line in open('sample_1000.csv'):
        link = line.split('\t')[0].replace('=HYPERLINK(\"','')
        link = link.replace('\")','')
        sample.append(link)
    print 'sample loaded'
    for line in open(filename):
        try:
            lj = json.loads(line.strip('\n'))
            #print lj['fetch_url']
            if lj['fetch_url'] in sample:
                soup = BeautifulSoup(lj['fetch_data'])
                print lj['fetch_url']
                downloadTables(soup, lj['fetch_url'] )
        except Exception,e:
            print str(e)


def getTitle1(url):
    header = {'User-Agent': 'Mozilla/5.0'} #Needed to prevent 403 error on Wikipedia
    req = urllib2.Request(url,headers=header)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)
    
    h3 = 'h3'
    for section in soup.findAll("table", { "class" : "wikitable" }):
        print section.name
        table_title = section.find_previous_sibling('h3')
        if not table_title:
            table_title = section.find_previous_sibling('h2')
        if not table_title:
            table_title = soup.find('h1')
        table_title = table_title.text
        print table_title
        page_title = soup.find('h1').text

        #results[title] = [{header: td.text for header, td in zip(headers, row.find_all('td'))} for row in rows[1:]]
        #pprint(results)

def annotationSampling2(sample_size):
    count = 0
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    total_tables = len(db.st2.find({'min_len_coverage':{'$gt':4}}).distinct('url'))
    categs = db.st2.find({'min_len_coverage':{'$gt':4}}).distinct('category')
    categs_dict = {}
    total_categs_dict = {}
    for categ in categs:
        total_categs_dict[categ] = len(db.st2.find({'min_len_coverage':{'$gt':4},'category':categ}).distinct('url'))
        categs_dict[categ] = {}
        levels = db.st2.find({'min_len_coverage':{'$gt':4},'category':categ}).distinct('level')
        for level in levels:
            categs_dict[categ][level] = len(db.st2.find({'min_len_coverage':{'$gt':4},'level':level, 'category': categ}).distinct('url'))
    print total_tables, total_categs_dict
    for categ in categs_dict:
        print categ, categs_dict[categ]
    final_sampling = {}
    print "Sampling..........."
    for categ in categs:
        final_sampling[categ] = {}
        categ_share = float (total_categs_dict[categ] ) / float(total_tables)
        categ_sample = categ_share * sample_size
        print categ, categ_sample
        for level in categs_dict[categ].keys():
            level_share = float(categs_dict[categ][level]) / float(total_categs_dict[categ])
            print level, categ, total_categs_dict[categ],categs_dict[categ][level], level_share
            final_sampling[categ][level] = level_share * categ_sample
            if final_sampling[categ][level] < 1:
                final_sampling[categ][level] = 1
            else:
                final_sampling[categ][level] = int (final_sampling[categ][level])     #max between sample and distinct urls?
            count += final_sampling[categ][level]
    for categ in final_sampling.keys():
        print categ, final_sampling[categ]
    print count
    return final_sampling

def createSample2():
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    final_sampling = annotationSampling2(1000) #94 is the number to return 100 sample size (due to min 1 by category))
    sample_list = []
    urls_list = []
    for categ in final_sampling.keys():
        print categ
        for level in final_sampling[categ].keys():
            print level
            total = db.st2.find({'min_len_coverage':{'$gt':4},'level':level, 'category': categ}).count()
            #print total, final_sampling[level][categ], len(db.st2.find({'min_len_coverage':{'$gt':4},'level':level, 'category': categ}).distinct('url'))
            completed = 0
            while completed < final_sampling[categ][level]:
                rand = random.randint(0,total)
                docs = db.st2.find({'min_len_coverage':{'$gt':4},'level':level, 'category': categ}).limit(1).skip(rand)
                for candidate in docs:
                    if candidate['url'] not in urls_list and candidate['url'].endswith('etymologies')==False and 'counties' not in candidate['url']:
                        sample_list.append(candidate['_id'])
                        urls_list.append(candidate['url'])
                        completed += 1
    db.tables_sample1002.drop()
    for table in sample_list:
        doc = db.st2.find_one({'_id':table})
        db.tables_sample1002.insert(doc)

#'relational':True,'$where':'this.relations.length < 2','max_coverage':{'$gt':1},'tables_num':1,'level':level, 'category': categ
def annotationSampling(sample_size):
    count = 0
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    #{'triples':{'$exists':1},'$where':'this.triples.length < 2','max_coverage':{'$gt':1},'tables_num':1,'level':level, 'category': categ}
    total_tables = len(db.st2.find({'relational':True,'$where':'this.relations.length < 6','max_coverage':{'$gt':1},'tables_num':1}).distinct('url'))
    levels = db.st2.find({'relational':True,'$where':'this.relations.length < 6','max_coverage':{'$gt':1},'tables_num':1}).distinct('level')
    levels_dict = {}
    total_levels_dict = {}
    for level in levels:
        total_levels_dict[level] = len(db.st2.find({'relational':True,'$where':'this.relations.length < 6','max_coverage':{'$gt':1},'tables_num':1,'level':level}).distinct('url'))
        levels_dict[level] = {}
        categories = db.st2.find({'relational':True,'$where':'this.relations.length < 6','max_coverage':{'$gt':1},'tables_num':1, 'level':level}).distinct('category')
        for categ in categories:
            levels_dict[level][categ] = len(db.st2.find({'relational':True,'$where':'this.relations.length < 6','max_coverage':{'$gt':1},'tables_num':1,'level':level, 'category': categ}).distinct('url'))
    print total_tables, total_levels_dict
    for level in levels_dict:
        print level, levels_dict[level]
    final_sampling = {}
    print "Sampling..........."
    for level in levels:
        final_sampling[level] = {}
        level_share = float (total_levels_dict[level] ) / float(total_tables)
        level_sample = level_share * sample_size
        print level, level_sample
        for categ in levels_dict[level].keys():
            categ_share = float(levels_dict[level][categ]) / float(total_levels_dict[level])
            print categ, level, total_levels_dict[level],levels_dict[level][categ], categ_share
            final_sampling[level][categ] = categ_share * level_sample
            if final_sampling[level][categ] < 1:
                final_sampling[level][categ] = 1
            else:
                final_sampling[level][categ] = int (final_sampling[level][categ])     #max between sample and distinct urls?
            count += final_sampling[level][categ]
    for level in final_sampling.keys():
        print level, final_sampling[level]
    print count
    return final_sampling
    
def annotationSamplingT(sample_size):
    count = 0
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    #{'triples':{'$exists':1},'$where':'this.triples.length < 2','max_coverage':{'$gt':1},'tables_num':1,'level':level, 'category': categ}
    total_tables = len(db.st2.find({'triples':{'$exists':1},'$where':'this.triples.length < 2','max_coverage':{'$gt':1},'tables_num':1}).distinct('url'))
    levels = db.st2.find({'triples':{'$exists':1},'$where':'this.triples.length < 2','max_coverage':{'$gt':1},'tables_num':1}).distinct('level')
    levels_dict = {}
    total_levels_dict = {}
    for level in levels:
        total_levels_dict[level] = len(db.st2.find({'triples':{'$exists':1},'$where':'this.triples.length < 2','max_coverage':{'$gt':1},'tables_num':1,'level':level}).distinct('url'))
        levels_dict[level] = {}
        categories = db.st2.find({'triples':{'$exists':1},'$where':'this.triples.length < 2','max_coverage':{'$gt':1},'tables_num':1, 'level':level}).distinct('category')
        for categ in categories:
            levels_dict[level][categ] = len(db.st2.find({'triples':{'$exists':1},'$where':'this.triples.length < 2','max_coverage':{'$gt':1},'tables_num':1,'level':level, 'category': categ}).distinct('url'))
    print total_tables, total_levels_dict
    for level in levels_dict:
        print level, levels_dict[level]
    final_sampling = {}
    print "Sampling..........."
    for level in levels:
        final_sampling[level] = {}
        level_share = float (total_levels_dict[level] ) / float(total_tables)
        level_sample = level_share * sample_size
        print level, level_sample
        for categ in levels_dict[level].keys():
            categ_share = float(levels_dict[level][categ]) / float(total_levels_dict[level])
            print categ, level, total_levels_dict[level],levels_dict[level][categ], categ_share
            final_sampling[level][categ] = categ_share * level_sample
            if final_sampling[level][categ] < 1:
                final_sampling[level][categ] = 1
            else:
                final_sampling[level][categ] = int (final_sampling[level][categ])     #max between sample and distinct urls?
            count += final_sampling[level][categ]
    for level in final_sampling.keys():
        print level, final_sampling[level]
    print count
    return final_sampling





def getJaccard(url, urls_list, minimum):
    url = url.replace('http://en.wikipedia.org/wiki/List_of_','').split('_')
    for each in urls_list:
        target = each.replace('http://en.wikipedia.org/wiki/List_of_','').split('_')
        if distance.jaccard(url,target) < minimum:
            return 1
    return 0


def createSample():
    black_list = []
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    final_sampling = annotationSampling(615) #94 is the number to return 100 sample size (due to min 1 by category))
    sample_list = []
    urls_list = []
    for level in final_sampling.keys():
        print level
        for categ in final_sampling[level].keys():
            print categ
            total = db.st2.find({'relational':True,'$where':'this.relations.length < 6','max_coverage':{'$gt':1},'tables_num':1,'level':level, 'category': categ}).count()
            #print total, final_sampling[level][categ], len(db.st2.find({'min_len_coverage':{'$gt':4},'level':level, 'category': categ}).distinct('url'))
            print total
            completed = 0
            while completed < final_sampling[level][categ]:
                #print 'total', completed
                rand = random.randint(1,total)
                docs = db.st2.find({'relational':True,'$where':'this.relations.length < 6','max_coverage':{'$gt':1},'tables_num':1,'level':level, 'category': categ}).limit(1).skip(rand-1)
                for candidate in docs:
                    if candidate['url'] not in urls_list and 'List_of_Top' not in candidate['url']: #and candidate['url'].endswith('etymologies')==False and 'counties' not in candidate['url']  and 'List_of_towns_in' not in candidate['url'] and 'List_of_cities_in' not in candidate['url']: #and 'List_of_ship' not in candidate['url']:
                        if level == 2:
                            similar = getJaccard(candidate['url'], urls_list, 0.7)
                        elif level == 3:
                            similar = getJaccard(candidate['url'], urls_list, 0.5)
                        elif level == 4:
                            similar = getJaccard(candidate['url'], urls_list, 0.3)
                        else:
                            similar = getJaccard(candidate['url'], urls_list, 0.3)
                        if similar and level < 5:
                            continue
                        sample_list.append(candidate['_id'])
                        urls_list.append(candidate['url'])
                        completed += 1
    db.tables_samplepairs.drop()
    for table in sample_list:
        doc = db.st2.find_one({'_id':table})
        db.tables_samplepairs.insert(doc)



def createSampleT():
    black_list = []
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    final_sampling = annotationSampling(205) #94 is the number to return 100 sample size (due to min 1 by category))
    sample_list = []
    urls_list = []
    for level in final_sampling.keys():
        print level
        for categ in final_sampling[level].keys():
            print categ
            total = db.st2.find({'triples':{'$exists':1},'$where':'this.triples.length < 2','max_coverage':{'$gt':1},'tables_num':1,'level':level, 'category': categ}).count()
            #print total, final_sampling[level][categ], len(db.st2.find({'min_len_coverage':{'$gt':4},'level':level, 'category': categ}).distinct('url'))
            completed = 0
            while completed < final_sampling[level][categ]:
                #print 'total', total
                rand = random.randint(1,total)
                docs = db.st2.find({'triples':{'$exists':1},'$where':'this.triples.length < 2','max_coverage':{'$gt':1},'tables_num':1,'level':level, 'category': categ}).limit(1).skip(rand-1)
                for candidate in docs:
                    if candidate['url'] not in urls_list and candidate['url'].endswith('etymologies')==False and 'counties' not in candidate['url']  and 'List_of_towns_in' not in candidate['url'] and 'List_of_cities_in' not in candidate['url']: #and 'List_of_ship' not in candidate['url']:
                        if level == 2:
                            similar = getJaccard(candidate['url'], urls_list, 0.5)
                        elif level == 3:
                            similar = getJaccard(candidate['url'], urls_list, 0.3)
                        elif level == 4:
                            similar = getJaccard(candidate['url'], urls_list, 0.3)
                        else:
                            similar = getJaccard(candidate['url'], urls_list, 0.3)
                        if similar and level < 5:
                            continue
                        sample_list.append(candidate['_id'])
                        urls_list.append(candidate['url'])
                        completed += 1
    db.tables_sampletriples.drop()
    for table in sample_list:
        doc = db.st2.find_one({'_id':table})
        db.tables_sampletriples.insert(doc)

def alternateSample():
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    size = 100
    count = 0
    sample_list = []
    urls_list = []
    total = db.st2.find({'min_len_coverage':{'$gt':4}}).count()
    while len(sample_list) < size:
        rand = random.randint(0,total)
        docs = db.st2.find({'min_len_coverage':{'$gt':4}}).limit(1).skip(rand)
        for candidate in docs:
            if candidate['url'] not in urls_list:
                count += 1
                print count
                sample_list.append(candidate['_id'])
                urls_list.append(candidate['url'])
    db.tables_sample1003.drop()
    for table in sample_list:
        doc = db.st2.find_one({'_id':table})
        db.tables_sample1003.insert(doc)


def createSampleRand():
    sample_size = 600
    black_list = []
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    #final_sampling = annotationSampling(615) #94 is the number to return 100 sample size (due to min 1 by category))
    sample_list = []
    urls_list = []
    total = db.st2.find({'relational':True,'$where':'this.relations.length < 6','max_coverage':{'$gt':1},'tables_num':1}).count()
    completed = 0
    while completed < sample_size:
        print completed
        rand = random.randint(1,total)
        docs = db.st2.find({'relational':True,'$where':'this.relations.length < 6','max_coverage':{'$gt':1},'tables_num':1}).limit(1).skip(rand-1)
        for candidate in docs:
            if candidate['url'] not in urls_list: #and candidate['url'].endswith('etymologies')==False and 'counties' not in candidate['url']  and 'List_of_towns_in' not in candidate['url'] and 'List_of_cities_in' not in candidate['url']: #and 'List_of_ship' not in candidate['url']:
                similar = getJaccard(candidate['url'], urls_list, 0.7)
                if similar:
                    continue
                sample_list.append(candidate['_id'])
                urls_list.append(candidate['url'])
                completed += 1
    db.tables_randompairs.drop()
    for table in sample_list:
        doc = db.st2.find_one({'_id':table})
        db.tables_randompairs.insert(doc)


def createSampleSim():
    sample_size = 600
    black_list = []
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    #final_sampling = annotationSampling(615) #94 is the number to return 100 sample size (due to min 1 by category))
    sample_list = []
    urls_list = []
    total = db.st2.find({'relational':True,'$where':'this.relations.length < 3','max_coverage':{'$gte':1},'tables_num':1}).count()
    completed = 0
    for candidate in db.st2.find({'relational':True,'$where':'this.relations.length < 3','max_coverage':{'$gte':1},'tables_num':1}):
        print completed
        if candidate['url'] not in urls_list and 'Notes' not in candidate['relations'][0]: #and candidate['url'].endswith('etymologies')==False and 'counties' not in candidate['url']  and 'List_of_towns_in' not in candidate['url'] and 'List_of_cities_in' not in candidate['url']: #and 'List_of_ship' not in candidate['url']:
            similar = getJaccard(candidate['url'], urls_list, 0.5)
            if not similar:
                sample_list.append(candidate['_id'])
                urls_list.append(candidate['url'])
                completed += 1
    db.tables_randompairs.drop()
    for table in sample_list:
        doc = db.st2.find_one({'_id':table})
        db.tables_randompairs.insert(doc)




def getListCategory(url):
    db = pymongo.MongoClient('localhost:54321').wikitables2017
    header = {'User-Agent': 'Mozilla/5.0'} #Needed to prevent 403 error on Wikipedia
    req = urllib2.Request(url,headers=header)
    page = urllib2.urlopen(req)
    soup = BeautifulSoup(page)
    h2 = 'h2'
    categories = {}
    for section in soup.findAll("h2"):
        if section.text != 'Contents' and section.text!='Navigation menu':
            categories[section.text] = []
    for section in soup.findAll("h2"):
        if section.text in categories.keys():
            print section.text
            try:
                next = section.nextSibling
                while next.name != "h2":
                    try:
                        for link in next.findAll('a', href=True):
                            if link['href'].startswith('/wiki/'):
                                categories[section.text].append(link['href'])
                    except Exception,e:
                        print str(e)
                    next = next.nextSibling
            except Exception,e:
                print str(e)
    for key in categories.keys():
        db.list_categories.insert({'category':key,'childs':categories[key]})
        #print '\n\n\n',key, '----------------------'
        #print '\n'.join(categories[key])

def getTitle(table):
        table_title = None
        table_title = table.find_previous_sibling('h3')
        if not table_title:
            table_title = table.find_previous_sibling('h2')
        return table_title


if __name__ == '__main__':
    wiki = "http://en.wikipedia.org/wiki/List_of_lists_of_lists"
    parseListofLists(wiki)
    setAllpairs('/media/popstar/hdd3/fureteur/fureteur2017_2_out')
    getKeyColumn()
    mapEntities()
    getWikiToFreebase()
    getWikiLink()
    createEntityPairs()
    getCoverage()
    getListCategory('https://en.wikipedia.org/wiki/List_of_lists_of_lists')
    getRootList()
    createSample()
    createSampleSim()
    testSampleT()

