from config import ENTITY_INDEX_DIR, RELATION_INDEX_DIR, INDEX_DIR, ENT_DOCS_LEN, REL_DOCS_LEN
import lucene_basic
import simplejson as json
import cPickle as pickle
import os
import mmap


def entities_indexing(filename, docs_len, l):
    print filename
    count = 0
    with open(filename, "r+b") as f:
        map = mmap.mmap(f.fileno(), 0, prot=mmap.PROT_READ)
        for line in iter(map.readline, ""): 
            count += 1
            doc = json.loads(line.strip('\n'))
            try:
                docs_len[doc['mid']] += len(doc['text_sent'].split(' '))
            except KeyError:
                docs_len[doc['mid']] = len(doc['text_sent'].split(' '))
            doclist = [{'field_name':key, 'field_value':value, 
            'field_type': l.FIELDTYPE_TEXT_TVP if key == 'text_sent' else l.FIELDTYPE_ID }
            for key, value in doc.iteritems()]
            l.add_document(doclist)
            if count % 2000000 == 0:
                print count
                #break
    return docs_len


if __name__ == '__main__':

    root = 'path to file'
    docs_len = {}
    l = lucene_basic.Lucene(ENTITY_INDEX_DIR)
    l.open_writer()
    for root, dirnames, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith('.json'):
                filepath = os.path.join(root, filename)
                try:
                    docs_len = entities_indexing(filepath, docs_len, l)
                    print 'docs_len size: ', len(docs_len)
                except Exception,e:
                    print 'ERROR:::::::::::::::::::::', str(e)
    l.close_writer()
    with open(ENT_DOCS_LEN, 'wb') as handle:
        pickle.dump(docs_len, handle, protocol=pickle.HIGHEST_PROTOCOL)
