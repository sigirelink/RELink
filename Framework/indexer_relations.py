from config import ENTITY_INDEX_DIR, RELATION_INDEX_DIR, INDEX_DIR, ENT_DOCS_LEN, REL_DOCS_LEN, ENT_REL_DOCS_FREQ
import lucene_basic
import simplejson as json
import cPickle as pickle
import os
import mmap

def relations_indexing(filename, docs_len, l):
    print filename
    count = 0
    with open(filename, "r+b") as f:
        map = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        for line in iter(map.readline, ""): 
            count += 1
            doc = json.loads(line.strip('\n'))
            doc['text_sent'] = doc['text']
            del(doc['text'])
            doc['mid'] = doc['mids']
            del(doc['mids'])
            try:
                docs_len[doc['mid']] += len(doc['text_sent'].split(' '))
            except KeyError:
                docs_len[doc['mid']] = len(doc['text_sent'].split(' '))
            doc['en_wikipedia'] = ''
            del(doc['en_wikipedia1'])
            del(doc['en_wikipedia2'])
            del(doc['mid1'])
            del(doc['mid2'])
            del(doc['curid1'])
            del(doc['curid2'])
            doclist = [{'field_name':key, 'field_value':value, 
            'field_type': l.FIELDTYPE_TEXT_TVP if key == 'text_sent' else l.FIELDTYPE_ID }
            for key, value in doc.iteritems()]
            l.add_document(doclist)
            if count % 2000000 == 0:
                print count
    print 'bla bla'
    return docs_len


def get_ent_freq(docs_len):
    ent_freq = {}
    for rel in docs_len:
        print rel
        ents = rel.split('#')
        try:
            ent_freq[ents[0]] += 1
        except KeyError:
            ent_freq[ents[0]] = 1
        try:
            ent_freq[ents[1]] += 1
        except KeyError:
            ent_freq[ents[1]] = 1
    return ent_freq


if __name__ == '__main__':
    root = 'path to clueweb extractions'
    docs_len = {}
    ent_freq = {}
    l = lucene_basic.Lucene(RELATION_INDEX_DIR)
    l.open_writer()
    for root, dirnames, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith('.json'):
                filepath = os.path.join(root, filename)
                try:
                    docs_len = relations_indexing(filepath, docs_len, l)
                    print 'docs_len size: ', len(docs_len)
                except Exception,e:
                    print 'ERROR:::::::::::::::::::::', str(e)
    l.close_writer()
    with open(REL_DOCS_LEN, 'wb') as handle:
        pickle.dump(docs_len, handle, protocol=pickle.HIGHEST_PROTOCOL)
    ent_freq = get_ent_freq(docs_len)
    with open(ENT_REL_DOCS_FREQ, 'wb') as handle:
        pickle.dump(ent_freq, handle, protocol=pickle.HIGHEST_PROTOCOL)
        
        
