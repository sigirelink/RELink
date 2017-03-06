"""
Class for relationship retrieval


"""
import itertools
import argparse
import json
import os
import cPickle as pickle
from config import QUERIES, ENTITY_INDEX_DIR, RELATION_INDEX_DIR, OUTPUT_DIR, INDEX_DIR, REL_DOCS_LEN, ENT_DOCS_LEN, ENT_REL_DOCS_FREQ
from scorer_rsdm import ScorerMRF
from lucene_basic import Lucene
from results import RetrievalResults
from retrieval2 import Retrieval
import math
import time

class RetrievalRSDM(Retrieval):
    def __init__(self, model, lambd=None):
        query_file = ''
        config = {'model': model,
                  'entity_index_dir': ENTITY_INDEX_DIR,
                  'relation_index_dir': RELATION_INDEX_DIR,  
                  'lambda': lambd,
                  'first_pass_num_docs': 500,
                  'num_docs': 500
                  }
        self._open_index()
        lambd_str = "_lambda" + "_".join([str(l) for l in lambd]) if lambd is not None else ""
        #self.run_id = model + lambd_str
        config['run_id'] = '__'
        config['output_file'] = OUTPUT_DIR + "/" + '__' + ".treceval"
        super(RetrievalRSDM, self).__init__(config)




    def _open_index(self):
        #self.elucene = Lucene(ENTITY_INDEX_DIR)
        self.rlucene = Lucene(RELATION_INDEX_DIR)
        #self.elucene.open_searcher()
        self.rlucene.open_searcher()

    def _close_index(self):
        self.elucene.close_reader()
        self.rlucene.close_reader()

    def _second_pass_scoring(self, res1, scorer, docs_len):
        """
        Returns second-pass scoring of documents.

        :param res1: first pass results
        :param scorer: scorer object
        :return: RetrievalResults object
        """
        #print "\tSecond pass scoring... "
        results = RetrievalResults()
        for group_id, orig_score in res1.get_scores_sorted():
            #print group_id
            
            score, features = scorer.score_doc(group_id,res1.group_docs, docs_len)
            #we need to change this: here we will just return the pt, po and pu for each query projection
            results.append(group_id, None, score, res1.group_docs_wiki[group_id], features)
        #print "done.........."
        scorer.phrase_freq = {}
        print len(results.scores), 'documents MRF scored.'
        return results

    def score_rel(self, e1_score, e2_score, rel_score, er_score1, er_score2):
        score = rel_score
        return score



    def do_retrieve(self, relquery, maps,rel_docs_len,ent_freq, lambda_params, store_json=True):
        """Score live query and outputs results."""
        if lambda_params:
            self.config['lambda'] = lambda_params
        rquery = Lucene.preprocess(' '.join(relquery.split('+')))

        print '\n'
        print "_______________________________"
        print "Live:: Retrieve Query:\n\t\t\tentA= " + rquery + "\n\t\t\t " 
        print "_______________________________"
        # First pass scoring for all
        t1 = time.time()
        res1_r12 = self._first_pass_scoring(self.rlucene, rquery, 'relation', None)   #Relationship scoring (it has to be always first!!)
        #self.config['first_pass_num_docs'] = 16000
        t2 = time.time()        
        print 'Executed in ', int(t2-t1), 'seconds.'

        scorer_r12 = ScorerMRF.get_scorer(self.rlucene,rquery, self.config)
        results_r12 = self._second_pass_scoring(res1_r12, scorer_r12, rel_docs_len)
        del(res1_r12)

        #RANKING time....
        final = self.ranking(results_r12, rel_docs_len, ent_freq, maps)
        return final


    def ranking(self, results_r12, rel_docs_len, ent_freq, maps):
        #/m/03m821b {'curid': '15419988', 'en_name': 'See Siang Wong', 'en_wikipedia': 'See_Siang_Wong'}

        r_sorted = results_r12.get_scores_sorted()
        final = RetrievalResults()
        print "number of relationships found: ", len(r_sorted)
        for pair_score in r_sorted:
            #print pair_score[0], pair_score[1]
            features = {}
            ents = pair_score[0].split('#')
            er_score1_wiki = '<' + maps[ents[0]]['en_wikipedia'] + '>'
            er_score2_wiki = '<' + maps[ents[1]]['en_wikipedia'] + '>'
            order = [er_score1_wiki, er_score2_wiki]
            final_score = pair_score[1]
            features['R_P_T_D'] = results_r12.features[pair_score[0]]['p_T_d'] 
            features['R_P_O_D'] = results_r12.features[pair_score[0]]['p_O_d'] 
            features['R_P_U_D'] = results_r12.features[pair_score[0]]['p_U_d'] 
            final.append('----'.join(order),None,final_score,None,features)
        return final

    def print_results(self,final_results):
        final_sorted = final_results.get_scores_sorted()
        print '\n____\nFINAL RESULTS:\n____\n'
        #print final_sorted
        for i in xrange(min(100,len(final_sorted))):
            ents = final_sorted[i][0].split('----')
            e1_len = len(ents[0])
            e2_len = len(ents[1])
            space = ' '
            space2 = ' '
            for j in xrange(60 - e1_len):
                space += ' '
            for j in xrange(60 - e2_len):
                space2 += ' '
            print ents[0],space,ents[1],space2, final_sorted[i][1] 


    def file_retrieve(self,queryfile, maps,rel_docs_len, ent_freq,lambda_params, store_json=True):
        """Scores queries and outputs results."""
        
        queries = self._load_queries_tsv('queries/'+ queryfile)
        # init output file
        #if os.path.exists(self.config['output_file']):
        #    os.remove(self.config['output_file'])
        out = open(self.config['output_file'], "w")
        print "Number of queries:", len(queries)
        for qid in sorted(queries):
            relquery = queries[qid]
            print "scoring [" + qid + "] " + queries[qid]
            results = self.do_retrieve(relquery, maps,rel_docs_len, ent_freq, lambda_params)
            #results.write_trec_format(qid, self.config['run_id'], out, self.config['num_docs'])
            results.write_letor_format(qid, self.config['run_id'], out, self.config['num_docs'])
        out.close()



def arg_parser(inputs):
    valid_models = ["lm", "rsdm"]
    parser = argparse.ArgumentParser()
    parser.add_argument("model", help="Model name", type=str, choices=valid_models)
    parser.add_argument("-qf", "--queryfile", help="Query file", type=str, default=QUERIES)
    parser.add_argument("-q", "--query", help="Relationship Query \"[qA]\";\"[qR]\";\"[qB]\" (comma-separated)", type=str, default=None)
    parser.add_argument("-l", "--lambd", help="Lambdas, comma separated values for ", type=str)
    args = parser.parse_args(inputs)
    return args


def main(rsdm,args, maps,rel_docs_len, ent_freq):
    #print 'Loading', args
    #print 'lambd\n', args.lambd
    #print 'query\n', args.query
    lambda_params = None
    relquery = None
    if args.lambd is not None:
        print 'Inserted lambdas...'
        lambdas = args.lambd.split(",")
        lambda_params = [float(l.strip()) for l in lambdas]
    if args.query:
        relquery = args.query

        start = time.time()
        results = rsdm.do_retrieve(relquery, maps,rel_docs_len, ent_freq, lambda_params)
        rsdm.print_results(results)
        print 'Query executed in', int(time.time() - start), 'seconds.'
    elif args.queryfile:
        lambd_str = "_lambda" + "_".join([str(l) for l in lambda_params]) if lambda_params is not None else ""
        run_id = '_rsdm_wiki__' 
        rsdm.config['output_file'] = OUTPUT_DIR + "/" + args.queryfile.replace('.tsv','_')+ run_id + ".letor"
        results = rsdm.file_retrieve(args.queryfile, maps,rel_docs_len, ent_freq, lambda_params)
        


if __name__ == '__main__':
    print '\n\n******************************************'
    print "\n\t\tRELATIONSHIP SEARCH\n\n"
    print '\n...Loading documents length dictionary.....\n'

    with open(REL_DOCS_LEN, 'rb') as handle:
        rel_docs_len = pickle.load(handle)
    with open(ENT_REL_DOCS_FREQ, 'rb') as handle:
        ent_freq = pickle.load(handle)

    with open('link to mid2wikipedia', 'rb') as handle:
        maps = pickle.load(handle)


    print 'Loaded!\n\n \n Relations: ', len(rel_docs_len), '\n Entities DF length: ', len(ent_freq)
    #rsdm = RetrievalRSDM(args.model, args.queryfile, relquery)
    rsdm = RetrievalRSDM("rsdm")
    while(1):
        print '******************************************'
        astr = raw_input('\n\nInsert relationship query: ')
        #try:
        main(rsdm, arg_parser(astr.split()), maps,rel_docs_len, ent_freq)
        #except Exception,e:
        #    print "ERROR::", str(e)
            
            

