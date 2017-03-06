from datetime import datetime

import sys
import json
import os
from lucene_basic import Lucene
from scorer import Scorer
from results import RetrievalResults


class Retrieval(object):
    def __init__(self, config):
        """
        Loads config file, checks params, and sets default values.

        :param config: JSON config file or a dictionary
        """
        # set configurations
        if type(config) == dict:
            self.config = config
        else:
            try:
                self.config = json.load(open(config))
            except Exception, e:
                print "Error loading config file: ", e
                sys.exit(1)

        # check params and set default values
        try:
            if 'entity_index_dir' not in self.config:
                raise Exception("entity_index_dir is missing")
            if 'relation_index_dir' not in self.config:
                raise Exception("relation_index_dir is missing")
            #if 'query_file' not in self.config:
            #    raise Exception("query_file is missing")
            if 'output_file' not in self.config:
                raise Exception("output_file is missing")
            if 'run_id' not in self.config:
                raise Exception("run_id is missing")
            if 'model' not in self.config:
                self.config['model'] = "lm"
            if 'num_docs' not in self.config:
                self.config['num_docs'] = 100
            #if 'field_id' not in self.config:
            #    self.config['field_id'] = Lucene.FIELDNAME_ID
            if 'first_pass_num_docs' not in self.config:
                self.config['first_pass_num_docs'] = 1000
            if 'first_pass_field' not in self.config:
                self.config['first_pass_field'] = Lucene.FIELDNAME_TEXT
            # model specific params
            if self.config['model'] == "lm" or self.config['model'] == "mlm" or self.config['model'] == "prms":
                if 'smoothing_method' not in self.config:
                    self.config['smoothing_method'] = "jm"
                # if 'smoothing_param' not in self.config:
                #     self.config['smoothing_param'] = 0.1
            """"
            if self.config['model'] == "mlm":
                if 'field_weights' not in self.config:
                    raise Exception("field_weights is missing")
            """
            if self.config['model'] == "prms":
                if 'fields' not in self.config:
                    raise Exception("fields is missing")

        except Exception, e:
            print "Error in config file: ", e
            sys.exit(1)

    def _open_index(self):
        self.elucene = Lucene(ENTITY_INDEX_DIR)
        self.rlucene = Lucene(RELATION_INDEX_DIR)
        self.elucene.open_searcher()
        self.rlucene.open_searcher()
        

    def _close_index(self):
        self.elucene.close_reader()
        self.rlucene.close_reader()


    def _load_queries_json(self):
        self.queries = json.load(open(self.config['query_file']))


    def _load_queries_tsv(self, queryfile):
        queries = {}
        for line in open(queryfile):
            data = line.strip('\n').split('\t')
            if data[2] != '<not applicable>':
                queries[data[0]] = data[2]
        return queries

    """
    def _ent_first_pass_scoring(self, elucene, equery):
        print "\tFirst pass scoring... ", equery
        results = elucene.score_query(equery, field_content=self.config['first_pass_field'],
                                     num_docs=self.config['first_pass_num_docs'])
        print results.num_docs()
        return results
    """
    def _first_pass_scoring(self, lucene, query, query_type, results_relation):
        """
        Returns first-pass scoring of documents.
        :param query: raw query
        :return RetrievalResults object
        """
        print "\nFirst pass scoring for: \t", query
        results = lucene.grouping_score_query(query, query_type, results_relation, field_content=self.config['first_pass_field'], num_docs=self.config['first_pass_num_docs'])
        return results
        




    def _second_pass_scoring(self, res1, scorer):
        """
        Returns second-pass scoring of documents.

        :param res1: first pass results
        :return: RetrievalResults object
        """
        print "\n\tSecond pass scoring... "
        results = RetrievalResults()
        for doc_id, orig_score in res1.get_scores_sorted():
            doc_id_int = res1.get_doc_id_int(doc_id)
            score = scorer.score_doc(doc_id, doc_id_int)
            results.append(doc_id, score)
        print "done"
        return results

    def retrieve(self):
        """Scores queries and outputs results."""
        s_t = datetime.now()  # start time
        total_time = 0.0

        self._load_queries()
        self._open_index()

        # init output file
        if os.path.exists(self.config['output_file']):
            os.remove(self.config['output_file'])
        out = open(self.config['output_file'], "w")

        for query_id in sorted(self.queries):
            # query = Query.preprocess(self.queries[query_id])
            query = Lucene.preprocess(self.queries[query_id])
            print "scoring [" + query_id + "] " + query
            # first pass scoring
            res1 = self._first_pass_scoring(self.lucene, query)
            # second pass scoring (if needed)
            if self.config['model'] == "lucene":
                results = res1
            else:
                scorer = Scorer.get_scorer(self.config['model'], self.lucene, query, self.config)
                results = self._second_pass_scoring(res1, scorer)
            # write results to output file
            results.write_trec_format(query_id, self.config['run_id'], out, self.config['num_docs'])

        # close output file
        out.close()
        # close index
        self._close_index()

        e_t = datetime.now()  # end time
        diff = e_t - s_t
        total_time += diff.total_seconds()
        time_log = "Execution time(sec):\t" + str(total_time) + "\n"
        print time_log


