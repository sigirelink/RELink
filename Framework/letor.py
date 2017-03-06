
import operator

def read_weights(folder, e_r, weights):
    for i in xrange(1,6):
        print i, folder, e_r
        for line in open(folder+'f'+str(i)+ e_r):
            if not line.startswith('#'):
                info = line.split(' ')
                for each in info:
                    values = each.split(':')
                    try:
                        weights[i].append(float(values[1]))
                    except KeyError:
                        weights[i] = [float(values[1])]
    return weights


def get_weights(folder):
    weights = {}
    weights = read_weights(folder, '_e', weights)
    weights = read_weights(folder, '_r', weights)
    return weights


def get_average_weights(weights):
    values = {}
    for i in xrange(1,7):
        values[i] = 0.0
    for each in weights:
        #print each, weights[each]
        for i in xrange(6):
            values[i+1] += weights[each][i]
    for v in values:
        values[v] = values[v] / 5
    print values


def read_dataset(dataset, fweights):
    output = open(dataset + '.merged', 'w')
    for line in open(dataset):
        row = []
        parsed = line.split('#')
        ind = parsed[0].index('1:')
        begin = parsed[0][:ind]
        info = parsed[0][ind:].strip(' ').split(' ')
        for feat in info:
            values = feat.split(':')
            row.append(float(values[1]))
        for i in xrange(len(row)):
            if i != 6:
                row[i] = row[i] * fweights[i]
            else:
                continue
        #print row
        newl = begin + '1:' + str(row[0] + row[1] + row[2]) + ' 2:' + str(row[3] + row[4] + row[5]) + ' 3:' + str(row[6]) + ' #' + parsed[1]
        output.write(newl)
        
        
def merge_feats(folder, weights):
    for i in xrange(1,6):
        read_dataset(folder+'f'+ str(i) + '.train', weights[i])
        read_dataset(folder+'f'+ str(i) + '.test', weights[i])


def rerank(ranked,testset):
    scores = {}
    test = {}
    for line in open(ranked):
        info = line.strip('\n').split('\t')
        try:
            scores[info[0]][int(info[1])] = float(info[2])
        except KeyError:
            scores[info[0]] = {}
            scores[info[0]][int(info[1])] = float(info[2])
    for line in open(testset):
        ind1 = line.index('qid:')
        ind2 = line.index(' 1:')
        qid = line[ind1+4:ind2]
        try:
            test[qid].append(line)
        except KeyError:
            test[qid] = [line]

    out = open(ranked.replace('scores','rank'),'w')
    for qid in scores:
        #print qid, scores[qid]
        sorted_ = sorted(scores[qid].iteritems(), key=operator.itemgetter(1), reverse=True)
        #print sorted_
        for i in xrange(min(100,len(sorted_))):
            #print sorted_[i][0]
            out.write(test[qid][sorted_[i][0]])
            
    out.close()

def create_shuffle(folder,run_file):
    sqid = []
    for line in open(folder+'reference.shuffled'):
        ind = line.index('qid:')
        ind2 = line.index(' 1:')
        qid = line[ind+4:ind2]
        if qid not in sqid:
            sqid.append(qid)
    print sqid
    run = {}
    for line in open(folder+run_file):
        ind = line.index('qid:')
        ind2 = line.index(' 1:')
        qid = line[ind+4:ind2]
        try:
            run[qid].append(line)
        except KeyError:
            run[qid] = [line]
    out = open(folder+run_file+'.shuffled','w')
    for qid in sqid:
        out.write(''.join(run[qid]))
    out.close()

