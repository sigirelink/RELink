#!/usr/bin/env python2
# -*- coding: utf-8 -*-
import warc
import codecs
from bs4 import BeautifulSoup
import nltk.data
import re, string
from nltk.tokenize import RegexpTokenizer
from nltk.tokenize import sent_tokenize
import sys
import pymongo
#clueweb09-en0000-00-00005	UTF-8	Domain Names	10871	10883	0.999885	0.000565	/m/09y1k
def readAnnotations(filename):
    segment = {} #annotations per segment, e.g., en000.warc.gz
    for line in open(filename):
        line = line.strip('\n')
        fields = line.split('\t')
        if fields[0] not in segment.keys():
            segment[fields[0]] = {}
        offsets = (int(fields[3]),int(fields[4]))
        segment[fields[0]][offsets] = {'mention':fields[2], 'encoding':fields[1], 'mid':fields[7], 'docid': fields[0], 'offset': offsets}
    return segment

def removeHtml(text):
    soup = BeautifulSoup(text)
    clean_text = ''
    clean_text = nltk.clean_html(text)
    return clean_text

def getSentence(text):
    return text
    
def getWords(text):
    words_list = []
    tokenizer = RegexpTokenizer(r'\w+')
    words_list = tokenizer.tokenize(text)
    #print words_list[-50:]
    return words_list

def getSentence(context, mention, word_before, word_after):
    tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    for sentence in tokenizer.tokenize(context):
        if mention in sentence:
            clean_sentence =  ' '.join(getWords(sentence))
            if word_before in clean_sentence or word_after in clean_sentence:
                return clean_sentence
    #print 'ERROR---------', mention, context
    return ''

def setRelations(mention_offset, sorted_offsets):
    relations = [offset for offset in sorted_offsets if offset[0] > mention_offset[1] and offset[1] < mention_offset[1]+2000]
    return relations

def getRelationText(context, mention, relations, mentions_dict, mention_doc_offset, docid, window_size):
    relations_list = []
    relations_control = []
    relation_text = ''
    mention_offset = context.find(mention)
    for offset in relations:
        clean_other = ' '.join(getWords(mentions_dict[offset]['mention']))
        relation_offset = context.find(clean_other)
        if mention_offset < relation_offset:
            relation_text = context[mention_offset + len(mention):relation_offset]
            relation_text = relation_text.strip().replace('  ', ' ')
            if type(relation_text) != unicode:
                relation_text = unicode(relation_text, errors='replace')
            #relation_text = unicode(relation_text, errors = 'replace')
            if relation_text != ' ' and relation_text != '':
                mid1 = mentions_dict[mention_doc_offset]['mid']
                mid2 = mentions_dict[offset]['mid']
                mids = ''
                if mid1 == mid2:
                    continue
                elif mid1 > mid2:
                    mids = mid2 + '#' + mid1
                else:
                    mids = mid1 + '#' + mid2
                if mids not in relations_control:
                    relations_control.append(mids)
                    relation_dict = { 'mids': mids , 'mid1': mid1, 'mid2': mid2 , 'docid': docid , 'window_size': window_size,  'text': relation_text}
                    relations_list.append(relation_dict)
    return relations_list


def readWarc(filename, segment):
    print 'readWarc....'
    relations_list = []
    mentions_list = []
    f = warc.open(filename)
    count = 0
    count_rec = 0
    count_men = 0
    for record in f:
        count_rec += 1
        #print count_rec
        if 'WARC-TREC-ID' in record:
            try:
                if record['WARC-TREC-ID'] in segment.keys():
                    sorted_offsets = sorted(segment[record['WARC-TREC-ID']].keys(),key=lambda x: x[0])
                    #print record.payload
                    #pos_init = record.payload.find('Canadian television network')
                    encoding = segment[record['WARC-TREC-ID']][sorted_offsets[0]]['encoding']
                    #print encoding
                    if encoding != 'UTF-8':
                        payload = record.payload.decode(encoding).encode('utf-8')
                    else:
                        payload = record.payload
                    for offset in sorted_offsets:
                        count_men += 1
                        mention = segment[record['WARC-TREC-ID']][offset]['mention']
                        if offset[0] - 2000 < 0: #avoid strange behaviour of negative indexes....
                            init_left = 0
                        else:
                            init_left = offset[0] - 2000
                        context_left = payload[init_left:offset[0]]
                        context_right = payload[offset[1]:offset[1]+2000]
                        context_left = removeHtml(context_left)
                        context_right = removeHtml(context_right)
                        words_left = getWords(context_left)
                        words_right = getWords(context_right)
                        window_8 = ' '.join(words_left[-8:]) + ' ' + mention + ' ' + ' '.join(words_right[:8])
                        if type(window_8) != unicode:
                            window_8 = unicode(window_8, errors = 'replace')
                        #window_50 = ' '.join(words_left[-50:]) + ' ' + mention + ' ' + ' '.join(words_right[:50])
                        #if type(window_50) != unicode:
                        #    window_50 = unicode(window_50, errors = 'replace')
                        window_sentence = getSentence(context_left + ' ' + mention + ' ' + context_right, mention, ' '.join(words_left[-2:]), ' '.join(words_right[:2]))
                        if type(window_sentence) != unicode:
                            window_sentence = unicode(window_sentence, errors = 'replace')
                        relations = setRelations(offset, sorted_offsets)
                        clean_mention = ' '.join(getWords(mention))
                        if len(relations) > 0:
                            #we just need to find relations after the mention (doing this for every mention to avoid repeated relations)
                            relations_list += getRelationText(clean_mention + ' '+ ' '.join(words_right[:8]), clean_mention, relations, segment[record['WARC-TREC-ID']], offset, record['WARC-TREC-ID'], 'eight')
                            #relations_list +=  getRelationText(clean_mention + ' '+ ' '.join(words_right[:50]), clean_mention, relations, segment[record['WARC-TREC-ID']], offset, record['WARC-TREC-ID'], 'fifty')
                            relations_list +=  getRelationText(window_sentence, clean_mention, relations, segment[record['WARC-TREC-ID']], offset, record['WARC-TREC-ID'], 'sentence')

                        segment[record['WARC-TREC-ID']][offset]['text_8'] = window_8
                        #segment[record['WARC-TREC-ID']][offset]['text_50'] = window_50
                        segment[record['WARC-TREC-ID']][offset]['text_sent'] = window_sentence
                        mentions_list.append(segment[record['WARC-TREC-ID']][offset])
                        
                        '''
                        print mention
                        print '....'
                        print window_sentence
                        print '....'
                        print window_8
                        print '....'
                        print window_50
                        print '_____________________'

                        if len(words_right) < 50 or len(words_left) < 50:
                            #print '------------------------------- less than 8'
                            count += 1
                        '''
                        
                #else:
                #    print 'Error: warc-trec-id not in annotations dictionary'
            except Exception,e:
                print str(e)

    print 'number of records, number of mentions, number of relations:', count_rec, count_men, len(relations_list)
    return mentions_list, relations_list

def listwikipedia():
    folders_list = ['en0004']
    for folder in folders_list:
        count = 81
        filename = ''
        while count < 99:
            count += 1
            if count < 10:
                filename = '0'+str(count)
            else:
                filename = str(count)
            print folder, filename
            try:
                cluefile = '/media/popstar/CLUEWEB09_B/ClueWeb09_English_1/' + folder + '/' + filename + '.warc.gz'
                facc = '/media/popstar/Novo volume1/ClueWeb09_English_1/' + folder + '/' + filename + '.anns.tsv'
                segs = readAnnotations(facc)
                mentions_list, relations_list = readWarc(cluefile, segs)
                print 'writing to mongodb.....'
                db = pymongo.MongoClient('localhost:27321').structured
                db.entities_0004.insert(mentions_list)
                db.relations_0004.insert(relations_list)
            except Exception,e:
                print 'ERROR:: ', str(e)



if __name__ == '__main__':
    print 'structured_search'
    cluefile = 'path to /CLUEWEB09_B/ClueWeb09_English_1/en0000/00.warc.gz'
    facc = 'path to /ClueWeb09_English_1/en0000/00.anns.tsv'
    listwikipedia()
    folder = 'enwp01'
    filename = '95'
    try:
        cluefile = '/media/popstar/CLUEWEB09_B/ClueWeb09_English_1/' + folder + '/' + filename + '.warc.gz'
        facc = '/media/popstar/Novo volume1/ClueWeb09_English_1/' + folder + '/' + filename + '.anns.tsv'
        segs = readAnnotations(facc)
        mentions_list, relations_list = readWarc(cluefile, segs)
        print 'writing to mongodb.....'
        db = pymongo.MongoClient('localhost:27123').structured
        #db.wiki_entities.insert(mentions_list)
        db.wiki_relations.insert(relations_list)
    except Exception,e:
        print 'ERROR:: ', str(e)
