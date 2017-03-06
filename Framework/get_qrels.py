import cPickle as pickle

def number_qald(qald_run):
    output = open(qald_run.replace('original',''),'w')
    for line in open(qald_run):
        data = line.strip('\n').split('#')
        query = data[1].split('\t')[0].replace(' ','')
        if query.split('-')[0] == 'QALD2_te':
            print query
            data = line.split(' ')
            data[1] = data[1] +'00'
            output.write(' '.join(data))
        else:
            output.write(line)
    output.close()


def qrel_qald(qald_run, qrels):
    errors = {}
    rights = {}
    output = open(qald_run.replace('.letor','qrels.letor'),'w')
    for line in open(qald_run):
        data = line.strip('\n').split('#')
        query = data[1].split('\t')[0].replace(' ','')
        
        if query in qrels.keys():
            print query
            rels = data[1].split('\t')[1].split('----')
            #print rels
            found = 0
            if query not in rights:
                rights[query] = 0
            #print rels
            for each in qrels[query]:
                if rels[0] in each and rels[1] in each:
                    #print query, rels
                    rights[query] += 1
                    found = 1
                    break
            if found == 1:
                output.write('1'+line)
            else:
                output.write('0'+line)
        else:
            errors[query] = 1
            output.write('0'+line)
            #print 'error---', query
    output.close()
    count = 0
    print 'ERRORS::', errors, len(errors)

def load_qrels(qrel_file):###
    qrels = {}
    for line in open(qrel_file):
        info = line.strip('\n').split('\t')
        try:
            rels = info[1].replace('[','').replace(']','').split(' , ')
            qrels[info[0]].append(rels)
        except KeyError:
            qrels[info[0]] = [rels]
    return qrels


def get_mids(qrels, maps_file):
    qrel_mids = {}
    with open(maps_file, 'rb') as handle:
        maps = pickle.load(handle)
    for q in qrels:
        for rel in qrels[q]:
            wiki1 = rel[0].replace('<','').replace('>','')
            wiki2 = rel[1].replace('<','').replace('>','')
            mid1 = None
            mid2 = None
            try:
                mid1 = maps[wiki1]
            except KeyError:
                print 'no mid------', q, wiki1
            try:
                mid2 = maps[wiki2]
            except KeyError:
                print 'no mid------', q, wiki2
            if mid1 and mid2:
                if mid1 > mid2:
                    mids = mid2 + '#' + mid1
                else:
                    mids = mid1 + '#' + mid2
                try:
                    qrel_mids[q].append(mids)
                except KeyError:
                    qrel_mids[q] = [mids]
    return qrel_mids

def getCoverage(qrel_mids, relation_doc_len):
    match = {}
    with open(relation_doc_len, 'rb') as handle:
        rel_docs_len = pickle.load(handle)
    for query in qrel_mids:
        for mids in qrel_mids[query]:
            if mids in rel_docs_len:
                try:
                    match[query].append(mids)
                except KeyError:
                    match[query] = [mids]

    print len(match)
    for each in match:
        print len(match[each])


def qrel_wikitables(wiki_run, qrels):
    rights = {}
    errors = {}
    output = open(wiki_run.replace('.letor','qrels.letor'),'w')
    #print qrels
    for line in open(wiki_run):
        data = line.strip('\n').split('#')
        query = data[1].split('\t')[0].replace(' ','')
        print '-'+query+'-'
        
        if query in qrels:
            rels = data[1].split('\t')[1].split('----')
            print rels
        else:
            errors[query] = 1
            #print 'error---', query
    print 'ERRORS::', qrels.keys(), len(qrels)
    output.close()
    count = 0

def qrels_fromrun(run, out):
    for line in open(run):
        if line.startswith('1'):
            info = line.split('#')
            qr = info[1].replace('\t<','\t[<').replace('>----<', '> , <').replace('>\n','>]\t1').strip(' ')
            out.write(qr+'\n')

