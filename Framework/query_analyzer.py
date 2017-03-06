
from __future__ import division
import math
from lucene_basic import Lucene
import time

class Scorer(object):


    SCORER_DEBUG = 0

    def __init__(self, lucene, query, params):
        self.lucene = lucene
        self.query = query
        self.params = params
        self.lucene.open_searcher()
        """
        @todo consider the field for analysis
        """
        # NOTE: The analyser might return terms that are not in the collection.
        # These terms are filtered out later in the score_doc functions.
        self.query_terms = lucene.analyze_query(self.query) if query is not None else None
        print self.query_terms

        self.len_C = self.lucene.get_coll_length('text_sent')
        self.t_coll_freq = self.get_terms_freq()

    def get_terms_freq(self):
        t_coll_freq = {}
        for t in set(self.query_terms):
            tf_t_C = self.lucene.get_coll_termfreq(t, 'text_sent')
            t_coll_freq[t] = tf_t_C
        return t_coll_freq


    @staticmethod
    def get_scorer(model, lucene, query, params):
        """
        Returns Scorer object (Scorer factory).

        :param model: accepted values: lucene, lm or mlm
        :param lucene: Lucene object
        :param query: raw query (to be analyzed)
        :param params: dict with models parameters
        """
        if model == "lm":
            print "\tLM scoring ... "
            return ScorerLM(lucene, query, params)

        else:
            raise Exception("Unknown model '" + model + "'")


class ScorerLM(Scorer):
    def __init__(self, lucene, query, params):
        super(ScorerLM, self).__init__(lucene, query, params)
        self.smoothing_method = params.get('smoothing_method', "jm").lower()
        if (self.smoothing_method != "jm") and (self.smoothing_method != "dirichlet"):
            raise Exception(self.params['smoothing_method'] + " smoothing method is not supported!")
        self.tf = {}



    @staticmethod
    def get_dirichlet_prob(tf_t_d, len_d, tf_t_C, len_C, mu):
        """
        Computes Dirichlet-smoothed probability
        P(t|theta_d) = [tf(t, d) + mu P(t|C)] / [|d| + mu]

        :param tf_t_d: tf(t,d)
        :param len_d: |d|
        :param tf_t_C: tf(t,C)
        :param len_C: |C| = \sum_{d \in C} |d|
        :param mu: \mu
        :return:
        """
        if mu == 0:  # i.e. field does not have any content in the collection
            return 0
        else:
            p_t_C = tf_t_C / len_C if len_C > 0 else 0
            return (tf_t_d + mu * p_t_C) / (len_d + mu)

    def get_tf(self, group_id, group_docs, qterms):
        #print 'get_tf-------------'
        if group_id not in self.tf:
            self.tf[group_id] = {}
            self.tf[group_id] = self.lucene.get_group_termfreqs(group_docs, qterms)
        return self.tf[group_id]

    def get_term_prob(self, group_id, group_docs, t, docs_len, tf_t_d=None, tf_t_C=None, tf = None):
        """
        Returns probability of a given term for the given group.

        :param lucene_doc_id: internal Lucene document ID
        :param field: entity field name, e.g. <dbo:abstract>
        :param t: term
        :return: P(t|d_f)
        """
        field = 'text_sent'

        # Gets term freqs for field of document

        #print '\n TERMS -', group_id, tf

        len_d = docs_len[group_id]
        
        len_C = self.len_C
        
        #print 'DOCUMENT LENGTH: ', group_id, len_d, len_C
        #return

        tf_t_d = tf.get(t, 0) if tf_t_d is None else tf_t_d
        #print 'tf_t_d', tf_t_d
        tf_t_C = self.t_coll_freq[t] if tf_t_C is None else tf_t_C

        if self.SCORER_DEBUG:
            print "\t\t t=" + t + ", f=" + field
            print "\t\t\t Doc:  tf(t,D)=" + str(tf_t_d) + "\t |D|=" + str(len_d)
            print "\t\t\t Coll: tf(t,C)=" + str(tf_t_C) + "\t |C|=" + str(len_C)


        # Dirichlet smoothing
        if self.smoothing_method == "dirichlet":
            mu = self.params.get('smoothing_param', self.lucene.get_avg_len(field))
            p_t_d = self.get_dirichlet_prob(tf_t_d, len_d, tf_t_C, len_C, mu)
            if self.SCORER_DEBUG:
                print "\t\t\t Dirichlet smoothing:"
                print "\t\t\t mu:", mu
                print "\t\t\t Doc:  p(t|theta_d)=", p_t_d
        return p_t_d

    def get_term_probs(self, group_id, group_docs, docs_len):
        """
        Returns probability of all query terms for the given field.

        :param lucene_doc_id: internal Lucene document ID
        :param field: entity field name, e.g. <dbo:abstract>
        :return: dictionary of terms with their probabilities
        """
        #print '\t\t P(T|D)'
        qterms = set(self.query_terms)
        p_t_theta_d = {}
        if group_docs is not None:
            tf = self.get_tf(group_id, group_docs, qterms)
        for t in qterms:
            #print 't ', t, group_id
            #print "\tt:", tget_term_prob
            p_t_theta_d[t] = self.get_term_prob(group_id, group_docs, t, docs_len, tf=tf)
        return p_t_theta_d

    def score_doc(self, group_id, group_docs):
        """
        Scores the given document using LM.

        :param doc_id: document id
        :param lucene_doc_id: internal Lucene document ID
        :return float, LM score of document and query
        """
        if self.SCORER_DEBUG:
            print "Scoring group ID=" + group_id

        p_t_theta_d = self.get_term_probs(group_id, group_docs)
        if sum(p_t_theta_d.values()) == 0:  # none of query terms are in the field collection
            if self.SCORER_DEBUG:
                print "\t\tP(q|" + field + ") = None"
            return None
        # p(q|theta_d) = prod(p(t|theta_d)) ; we return log(p(q|theta_d))
        p_q_theta_d = 0
        for t in self.query_terms:
            # Skips the term if it is not in the field collection
            if p_t_theta_d[t] == 0:
                continue
            if self.SCORER_DEBUG:
                print "\t\tP(" + t + "|" + field + ") = " + str(p_t_theta_d[t])
            p_q_theta_d += math.log(p_t_theta_d[t])
        if self.SCORER_DEBUG:
            print "\tP(d|q)=" + str(p_q_theta_d)
        return p_q_theta_d

