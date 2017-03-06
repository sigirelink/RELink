
import operator


class RetrievalResults(object):
    """Class for storing retrieval scores for a given query."""
    def __init__(self):
        self.scores = {}
        # mapping from external to internal group_docs -s
        self.group_docs = {}
        self.group_docs_wiki = {}
        self.entities_dict = {}
        self.features = {}

    def append(self, group_id, docs, score, wikititle, features):
        """Adds document to the result list"""
        self.group_docs[group_id] = docs 
        self.scores[group_id] = score
        self.group_docs_wiki[group_id] = wikititle
        self.features[group_id] = features

    def append_entity(self, group_id, docs, score, wikititle, features):
        """Adds document to the result list"""
        self.group_docs[group_id] = docs 
        self.scores[group_id] = score
        self.group_docs_wiki[group_id] = wikititle
        self.features[group_id] = features

    def append_relation(self, group_id, docs, score, wikititle, features):
        """Adds document to the result list"""
        self.group_docs[group_id] = docs 
        self.scores[group_id] = score
        self.group_docs_wiki[group_id] = wikititle
        for ent in group_id.split('#'):
            self.entities_dict[ent] = None
        self.features[group_id] = features

    def increase(self, group_id, score):
        """Increases the score of a document (adds it to the results list
        if it is not already there)"""
        if group_id not in self.scores:
            self.scores[group_id] = 0
        self.scores[group_id] += score

    def num_docs(self):
        """Returns the number of documents in the result list."""
        return len(self.scores)

    def get_scores_sorted(self):
        """Returns all results sorted by score"""
        return sorted(self.scores.iteritems(), key=operator.itemgetter(1), reverse=True)

    def get_group_id_int(self, group_id):
        """Returns internal group_id for a given group_id."""
        if group_id in self.group_docs:
            return self.group_docs[group_id]
        return None

    def write_trec_format(self, query_id, run_id, out, max_rank=100):
        """Outputs results in TREC format"""
        rank = 1
        for group_id, score in self.get_scores_sorted():
            if rank <= max_rank:
                out.write(query_id + "\tQ0\t" + group_id + "\t" + str(rank) + "\t" + str(score) + "\t" + run_id + "\n")
            rank += 1

    def write_letor_format(self, query_id, run_id, out, max_rank=100):
        """Outputs results in LETOR format"""
        rank = 1
        for group_id, score in self.get_scores_sorted():
            if rank <= max_rank:
                out.write(" qid:" + query_id.split('-')[-1] + " 1:" + str(self.features[group_id]['R_P_T_D']) + " 2:" 
                 + str(self.features[group_id]['R_P_O_D']) + " 3:" + str(self.features[group_id]['R_P_U_D']) +  " # " + query_id + "\t" + group_id + "\n")
            else:
                break
            rank += 1

