from __future__ import division

import math


from lucene_basic import Lucene
from query_analyzer import ScorerLM


class ScorerMRF(object):
    DEBUG = 0

    TERM = "terms"
    ORDERED = "ordered"
    UNORDERED = "unordered"
    URI = "uris"
    SLOP = 6  # Window = 8

    def __init__(self, lucene, query, params):
        self.lucene = lucene
        self.params = params
        self.phrase_freq = {}
        self.query = query
        self.scorer_lm_term = ScorerLM(self.lucene, self.query, {'smoothing_method': "dirichlet"})
        self.instance_list = []
        self.__n_fields = None
        self.__bigrams = None
        self.__mlm_all_mapping = None
        self.bigrams = self.get_bigrams()

    def get_bigrams(self):
        """Returns all query bigrams."""
        __bigrams = []
        for i in range(0, len(self.scorer_lm_term.query_terms)-1):
            bigram = " ".join([self.scorer_lm_term.query_terms[i], self.scorer_lm_term.query_terms[i+1]])
            __bigrams.append(bigram)
        return __bigrams




    @staticmethod
    def get_scorer(lucene, query, params):
        """
        Returns Scorer object (Scorer factory).

        :param lucene: Lucene object 
        :param params: dict with models parameters
        """
        model = params['model']
        lambd = params['lambda']
        print "\n\nSecond pass scoring using " + model + "...\n"
        if (model == "rsdm"):
            params['lambda'] = [0.8, 0.1, 0.1] if lambd is None else lambd
            return ScorerRSDM(lucene, query, params)
        elif (model == "lm") or (model == "prms") or (model == "mlm-all") or (model == "mlm-tc"):
            params['lambda'] = [1.0, 0.0, 0.0] if lambd is None else lambd
            return ScorerFSDM(lucene, params)
        elif (model == "sdm") or (model == "fsdm"):
            params['lambda'] = [0.8, 0.1, 0.1] if lambd is None else lambd
            return ScorerFSDM(lucene, params)
        elif (model == "lm_elr") or (model == "prms_elr") or (model == "mlm-tc_elr") or (model == "mlm-all_elr"):
            params['lambda'] = [0.9, 0.0, 0.0, 0.1] if lambd is None else lambd
            return ScorerELR(lucene, params)
        elif (model == "sdm_elr") or (model == "fsdm_elr"):
            params['lambda'] = [0.8, 0.05, 0.05, 0.1] if lambd is None else lambd
            return ScorerELR(lucene, params)
        else:
            raise Exception("Unknown model '" + model + "'")


    def set_phrase_freq(self, clique_type, c, f, group_docs):
        """Sets document and collection frequency for phrase."""
        #print 'set_phrase_freq', clique_type, c
        if clique_type not in self.phrase_freq:
            self.phrase_freq[clique_type] = {}
        if c not in self.phrase_freq.get(clique_type, {}):
            self.phrase_freq[clique_type][c] = {}
            if clique_type == self.ORDERED:
                doc_freq = self.lucene.get_doc_phrase_freq(c, f, 0, True)
            elif clique_type == self.UNORDERED:
                doc_freq = self.lucene.get_doc_phrase_freq(c, f, self.SLOP, False)

            self.phrase_freq[clique_type][c] = doc_freq
            self.phrase_freq[clique_type][c]['coll_freq'] = sum(doc_freq.values())


    def get_p_t_d(self, group_id, group_docs, docs_len):
        """
        p(t|d)

        :param t: term
        :param doc_id: entity id
        :return  p(t|d)
        """
        #print '\n\n\t get_p_t_d'
        p_t_d = {}
        p_t_d = self.scorer_lm_term.get_term_probs(group_id, group_docs, docs_len)
        return p_t_d

    def get_p_o_d(self, group_id, group_docs, docs_len):
        """
        p(o|d) for ordered search

        :param o: phrase (ordered search)
        :param field_weights: Dictionary {f: p_f_o, ...}
        :param doc_id: entity id
        :return  p(o|d)
        """
        #print '\n\n\t get_p_o_d', group_id
        field = 'text_sent'
        p_o_d = {}
        for o in self.bigrams:
            if self.ORDERED not in self.phrase_freq:
                #print 'ordered not in phrase_freq'
                self.set_phrase_freq(self.ORDERED, o, field, group_docs[group_id])
            elif o not in self.phrase_freq[self.ORDERED]:
                #print 'o not in phrase_freq'
                self.set_phrase_freq(self.ORDERED, o, field, group_docs[group_id])
            #print group_id,group_docs
            #print self.phrase_freq[self.ORDERED][o]
            #if self.DEBUG:
            #    print "\to:", o
            tf_t_d = 0
            tf_t_C = self.phrase_freq[self.ORDERED][o].get('coll_freq', 0)

            for lucene_doc_id in group_docs[group_id]:
                tf_t_d += self.phrase_freq[self.ORDERED][o].get(lucene_doc_id, 0)
            p_o_d[o] = self.scorer_lm_term.get_term_prob(group_id, group_docs[group_id], o, docs_len, tf_t_d, tf_t_C)
        return p_o_d

    def get_p_u_d(self, group_id, group_docs, docs_len):
        """
        p(u|d) = for unordered search

        :param u: phrase (unordered search)
        :param doc_id: entity id
        :return  p(u|d)
        """
        #print '\n\n\t get_p_u_d'
        field = 'text_sent'
        p_u_d = {}
        for u in self.bigrams:
            if self.UNORDERED not in self.phrase_freq:
                #print 'ordered not in phrase_freq'
                self.set_phrase_freq(self.UNORDERED, u, field, group_docs[group_id])
            elif u not in self.phrase_freq[self.UNORDERED]:
                #print 'o not in phrase_freq'
                self.set_phrase_freq(self.UNORDERED, u, field, group_docs[group_id])
            #if self.DEBUG:
            #    print "\tu:", u
            tf_t_d = 0
            tf_t_C = self.phrase_freq[self.UNORDERED][u].get('coll_freq', 0)
            #print self.phrase_freq[self.ORDERED][o]
            #print group_docs
            for lucene_doc_id in group_docs[group_id]:
                tf_t_d += self.phrase_freq[self.UNORDERED][u].get(lucene_doc_id, 0)
            p_u_d[u] = self.scorer_lm_term.get_term_prob(group_id, group_docs[group_id], u, docs_len, tf_t_d=tf_t_d, tf_t_C=tf_t_C)
        return p_u_d
        

class ScorerRSDM(ScorerMRF):
    DEBUG_RSDM = 0

    def __init__(self, lucene, query, params):
        ScorerMRF.__init__(self, lucene, query, params)
        self.lambda_T = self.params['lambda'][0]
        self.lambda_O = self.params['lambda'][1]
        self.lambda_U = self.params['lambda'][2]
        #self.T = self.query_annot.T:

    def score_doc(self, group_id, group_docs, docs_len):
        """    
        P(q|e) = lambda_T sum_{t in T}P(t|d) + lambda_O sum_{o in O}P(o|d) + lambda_U sum_{u in U}P(u|d)
        P(t|d) = sum_{f in F} p(t|d_f) p(f|t)
        P(o|d) = sum_{f in F} p(o|d_f) p(f|o)
        P(u|d) = sum_{f in F} p(u|d_f) p(f|u)

        :param doc_id: document id
        :return: p(q|d)
        """
        #if self.DEBUG_RSDM:
        #print "RSDM Scoring doc ID=" + group_id#, self.phrase_freq
        #print 'p_T_d'
        p_T_d = 0.0
        if self.lambda_T != 0:
                p_t_d = self.get_p_t_d(group_id, group_docs[group_id], docs_len)
                if p_t_d:
                    for t in p_t_d:
                        if p_t_d[t] != 0:
                            p_T_d += math.log(p_t_d[t])
                    #print '\n\t\t\tp_T_d:', p_T_d
        #print 'p_O_d'
        p_O_d = 0.0
        if self.lambda_O != 0:
                p_o_d = self.get_p_o_d(group_id, group_docs, docs_len)         
                if p_o_d:
                    for o in p_o_d:
                        if p_o_d[o] != 0:
                            p_O_d += math.log(p_o_d[o])
        #print 'p_U_d'
        p_U_d = 0.0
        if self.lambda_U != 0:
                p_u_d = self.get_p_u_d(group_id, group_docs, docs_len)
                if p_u_d:
                    for u in p_u_d:
                        if p_u_d[u] != 0:
                            p_U_d += math.log(p_u_d[u])
        
        p_q_d = 0.0
        p_q_d = (self.lambda_T * p_T_d) + (self.lambda_O * p_O_d) + (self.lambda_U * p_U_d)
        """
        if self.DEBUG_RSDM:
            print "\t\t P(q|d):", p_q_d 
            print "\t\t\tP(T|d) = ", p_T_d 
            print "\t\t\tP(O|d) = ",  p_O_d 
            print "\t\t\tp(U|d) = ",  p_U_d ,'\n\n'
        """
        features  = {'p_T_d': p_T_d, 'p_O_d': p_O_d, 'p_U_d': p_U_d}
        return p_q_d, features



