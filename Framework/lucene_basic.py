
import argparse
import lucene
from results import RetrievalResults
from PorterStemmerAnalyzer import PorterStemmerAnalyzer
from java.io import File
from java.util import HashMap, TreeSet
from java.io import StringReader
from java.lang import StringBuilder
from org.apache.lucene.analysis.tokenattributes import CharTermAttribute
from org.apache.lucene.analysis.core import StopFilter
from org.apache.lucene.analysis.core import StopAnalyzer
from org.apache.lucene.analysis.standard import StandardTokenizer
from org.apache.lucene.analysis.standard import StandardAnalyzer
from org.apache.lucene.analysis.shingle import ShingleAnalyzerWrapper
from org.apache.lucene.document import Document
from org.apache.lucene.document import Field
from org.apache.lucene.document import FieldType
from org.apache.lucene.index import MultiFields
from org.apache.lucene.index import IndexWriter
from org.apache.lucene.index import IndexWriterConfig
from org.apache.lucene.index import DirectoryReader 
from org.apache.lucene.index import Term
from org.apache.lucene.index import TermContext
from org.apache.lucene.queryparser.classic import QueryParser
from org.apache.lucene.search import IndexSearcher
from org.apache.lucene.search import BooleanClause
from org.apache.lucene.search import TermQuery
from org.apache.lucene.search import BooleanQuery
from org.apache.lucene.search import PhraseQuery
from org.apache.lucene.search.spans import SpanNearQuery
from org.apache.lucene.search.spans import SpanTermQuery
from org.apache.lucene.search import FieldValueFilter
from org.apache.lucene.search.similarities import LMJelinekMercerSimilarity
from org.apache.lucene.search.similarities import LMDirichletSimilarity
from org.apache.lucene.store import SimpleFSDirectory
from org.apache.lucene.store import NIOFSDirectory
from org.apache.lucene.util import BytesRefIterator
from org.apache.lucene.util import Version
from org.apache.lucene.index import SlowCompositeReaderWrapper
from org.apache.lucene.search.grouping import GroupingSearch
from org.apache.lucene.search import Sort
from org.apache.lucene.search import MatchAllDocsQuery
from org.apache.lucene.analysis.en import PorterStemFilter
from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
# has java VM for Lucene been initialized
lucene_vm_init = False

#{u'wikipedia_title': u'W\u0456kipedia', u'text_sent': u'For example the URL http en wikipedia org wiki meatball WikiPedia will give this error because the meatball interwiki prefix is not marked as local in the interwiki table', u'docid': u'clueweb09-enwp00-00-00002', u'mid': u'/m/0d07ph', u'_id': u'54b60cb0e9e5736bf4b80b93'}

class Lucene(object):

    # default fieldnames for id and contents
    FIELDNAME_ID = "id"
    FIELDNAME_TEXT = "text_sent"
    FIELDNAME_DOC_ID = "docid"
    FIELDNAME_MID = "mid"


    # internal fieldtypes
    # used as Enum, the actual values don't matter
    FIELDTYPE_ID = "id"
    FIELDTYPE_ID_TV = "id_tv"
    FIELDTYPE_TEXT = "text"
    FIELDTYPE_TEXT_TV = "text_tv"
    FIELDTYPE_TEXT_TVP = "text_tvp"
    FIELDTYPE_TEXT_NTV = "text_ntv"
    FIELDTYPE_TEXT_NTVP = "text_ntvp"


    def __init__(self, index_dir, max_shingle_size=None):
        global lucene_vm_init

        if not lucene_vm_init:
            #lucene.initVM(vmargs=['-Djava.awt.headless=true'])
            lucene.initVM(vmargs=['-Djava.awt.headless=true','-Xms16g','-Xmx16g','-Xmn14g', '-XX:+UseParallelOldGC', '-XX:+DisableExplicitGC', '-XX:ParallelGCThreads=8'])#,'-XX:+UseParallelOldGC', '-XX:+ DisableExplicitGC'])#,'-XX:ParallelGCThreads =8'])
            lucene_vm_init = True
        #self.dir = SimpleFSDirectory(File(index_dir))
        self.dir =NIOFSDirectory(File(index_dir))
        self.max_shingle_size = max_shingle_size
        self.analyzer = None
        self.reader = None
        self.searcher = None
        self.writer = None
        self.ldf = None

    @staticmethod
    def get_version():
        """Get Lucene version."""

        return Version.LUCENE_48

    @staticmethod
    def preprocess(text):
        """Tokenize and stop the input text."""
        ts = StandardTokenizer(Lucene.get_version(), StringReader(text.lower()))
        #ts = StopFilter(Lucene.get_version(), ts,  StopAnalyzer.ENGLISH_STOP_WORDS_SET)
        ts = PorterStemFilter(ts)
        string_builder = StringBuilder()
        ts.reset()
        char_term_attr = ts.addAttribute(CharTermAttribute.class_)
        while ts.incrementToken():
            if string_builder.length() > 0:
                string_builder.append(" ")
            string_builder.append(char_term_attr.toString())
        return string_builder.toString()


    def get_analyzer(self):
        """Get analyzer."""
        if self.analyzer is None:
            std_analyzer = PorterStemmerAnalyzer(Lucene.get_version())
            if self.max_shingle_size is None:
                self.analyzer = std_analyzer
            else:
                self.analyzer = ShingleAnalyzerWrapper(std_analyzer, self.max_shingle_size)
        return self.analyzer

    def open_reader(self):
        """Open IndexReader."""
        if self.reader is None:
            self.reader = DirectoryReader.open(self.dir)

    def get_reader(self):
        return self.reader

    def close_reader(self):
        """Close IndexReader."""
        if self.reader is not None:
            self.reader.close()
            self.reader = None
        else:
            raise Exception("There is no open IndexReader to close")

    def open_searcher(self):
        """
        Open IndexSearcher. Automatically opens an IndexReader too,
        if it is not already open. There is no close method for the
        searcher.
        """
        if self.searcher is None:
            self.open_reader()
            self.searcher = IndexSearcher(self.reader, readOnly=True)

    def get_searcher(self):
        """Returns index searcher (opens it if needed)."""
        self.open_searcher()
        return self.searcher

    def set_lm_similarity_jm(self, method="jm", smoothing_param=0.1):
        """
        Set searcher to use LM similarity.

        :param method: LM similarity ("jm" or "dirichlet")
        :param smoothing_param: smoothing parameter (lambda or mu)
        """
        if method == "jm":
            similarity = LMJelinekMercerSimilarity(smoothing_param)
        elif method == "dirichlet":
            similarity = LMDirichletSimilarity(smoothing_param)
        else:
            raise Exception("Unknown method")

        if self.searcher is None:
            raise Exception("Searcher has not been created")
        self.searcher.setSimilarity(similarity)

    def open_writer(self):
        """Open IndexWriter."""
        
        if self.writer is None:
            #config = IndexWriterConfig(Lucene.get_version(), self.get_analyzer())
            analyzer = LimitTokenCountAnalyzer(PorterStemmerAnalyzer(), 1048576)
            config = IndexWriterConfig(Lucene.get_version(), analyzer)
            config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND)
            config.setRAMBufferSizeMB(48.0)
            #config.setUseCompoundFile(False)
            self.writer = IndexWriter(self.dir, config, autoCommit=False)
            #self.writer.setRAMBufferSizeMB(500)
        else:
            raise Exception("IndexWriter is already open")

    def close_writer(self):
        """Close IndexWriter."""
        if self.writer is not None:
            self.writer.commit()
            self.writer.close()
            self.writer = None
        else:
            raise Exception("There is no open IndexWriter to close")

    def add_document(self, contents):
        """
        Adds a Lucene document with the specified contents to the index.
        See LuceneEntityDocument.create_document() for the explanation of contents.
        """
        if self.ldf is None:  # create a single LuceneEntityDocument object that will be reused
            self.ldf = LuceneEntityDocument()
        #print "---------------------------------------------------------------------------"
        #print contents
        self.writer.addDocument(self.ldf.create_document(contents))

    def get_lucene_document_id(self, doc_id):
        """Loads a document from a Lucene index based ofield_text_tvpn its id."""
        self.open_searcher()
        query = TermQuery(Term(self.FIELDNAME_ID, doc_id))
        tophit = self.searcher.search(query, 1).scoreDocs
        #print 'tophits::: ', len(tophit)
        if len(tophit) == 1:
            return tophit[0].doc
        else:
            return None

    def get_document_id(self, lucene_doc_id):
        """Gets lucene document id and returns the document id."""
        self.open_reader()
        return self.reader.document(lucene_doc_id).get(self.FIELDNAME_ID)

    def print_document(self, lucene_doc_id, term_vect=False):
        """Prints document contents."""
        if lucene_doc_id is None:
            print "Document is not found in the index."
        else:
            doc = self.reader.document(lucene_doc_id)
            print "Document ID (field '" + self.FIELDNAME_ID + "'): " + doc.get(self.FIELDNAME_ID)

            # first collect (unique) field names
            fields = []
            for f in doc.getFields():
                if f.name() != self.FIELDNAME_ID and f.name() not in fields:
                    fields.append(f.name())

            for fname in fields:
                print fname
                for fv in doc.getValues(fname):  # printing (possibly multiple) field values
                    print "\t" + fv
                # term vector
                if term_vect:
                    print "-----"
                    termfreqs = self.get_doc_termfreqs(lucene_doc_id, fname)
                    for term in termfreqs:
                        print term + " : " + str(termfreqs[term])
                    print "-----"

    def get_lucene_query(self, query, field=FIELDNAME_TEXT):
        """Creates Lucene query from keyword query."""
        query = query.replace("(", "").replace(")", "").replace("!", "")
        
        return QueryParser(Lucene.get_version(), field, self.get_analyzer()).parse(query)

    def analyze_query(self, query, field=FIELDNAME_TEXT):
        """
        Analyses the query and returns query terms.

        :param query: query
        :param field: field name
        :return: list of query terms
        """
        qterms = []  # holds a list of analyzed query terms
        """
        ts = self.get_analyzer().tokenStream(field, query)
        ts = PorterStemFilter(ts)
        term = ts.addAttribute(CharTermAttribute.class_)
        """
        ts = StandardTokenizer(Lucene.get_version(), StringReader(query.lower()))
        #ts = StopFilter(Lucene.get_version(), ts,  StopAnalyzer.ENGLISH_STOP_WORDS_SET)
        ts = PorterStemFilter(ts)
        ts.reset()
        term = ts.addAttribute(CharTermAttribute.class_)
        while ts.incrementToken():
            qterms.append(term.toString())
        ts.end()
        ts.close()
        return qterms

    def get_id_lookup_query(self, id, field=None):
        """Creates Lucene query for searching by (external) document id."""
        if field is None:
            field = self.FIELDNAME_ID
        return TermQuery(Term(field, id))

    def get_and_query(self, queries):
        """Creates an AND Boolean query from multiple Lucene queries."""
        # empty boolean query with Similarity.coord() disabled
        bq = BooleanQuery(False)
        for q in queries:
            bq.add(q, BooleanClause.Occur.MUST)
        return bq

    def get_or_query(self, queries):                          
        """Creates an OR Boolean query from multiple Lucene queries."""
        # empty boolean query with Similarity.coord() disabled
        bq = BooleanQuery(False)
        for q in queries:
            bq.add(q, BooleanClause.Occur.SHOULD)
        return bq

    def get_phrase_query(self, query, field):
        """Creates phrase query for searching exact phrase."""
        phq = PhraseQuery()
        for t in query.split():
            phq.add(Term(field, t))
        return phq

    def get_span_query(self, terms, field, slop, ordered=True):
        """
        Creates near span query

        :param terms: list of terms
        :param field: field name
        :param slop: number of terms between the query terms
        :param ordered: If true, ordered search; otherwise unordered search
        :return: lucene span near query
        """
        span_queries = []
        for term in terms:
            span_queries.append(SpanTermQuery(Term(field, term)))
        span_near_query = SpanNearQuery(span_queries, slop, ordered)
        return span_near_query

    def get_doc_phrase_freq(self, phrase, field, slop, ordered):

        #print 'get_doc_phrase_freq:', phrase, slop
        # creates span near query
        span_near_query = self.get_span_query(phrase.split(" "), field, slop=slop, ordered=ordered)

        # extracts document frequency
        self.open_searcher()
        index_reader_context = self.searcher.getTopReaderContext()
        term_contexts = HashMap()
        terms = TreeSet()
        span_near_query.extractTerms(terms)
        for term in terms:
            term_contexts.put(term, TermContext.build(index_reader_context, term))
        leaves = index_reader_context.leaves()
        doc_phrase_freq = {}
        # iterates over all atomic readers
        for atomic_reader_context in leaves:
            bits = atomic_reader_context.reader().getLiveDocs()
            spans = span_near_query.getSpans(atomic_reader_context, bits, term_contexts)
            while spans.next():
                lucene_doc_id = spans.doc()
                #doc_id = atomic_reader_context.reader().document(lucene_doc_id).get(self.FIELDNAME_ID)
                #print len(doc_phrase_freq)
                if lucene_doc_id not in doc_phrase_freq:
                    doc_phrase_freq[lucene_doc_id] = 1
                else:
                    doc_phrase_freq[lucene_doc_id] += 1
        return doc_phrase_freq

    #def get_id_filter(self):
    #    return FieldValueFilter(self.FIELDNAME_ID)

    def __to_retrieval_results(self, scoredocs, field_id=FIELDNAME_ID):
        """Converts Lucene scoreDocs results to RetrievalResults format."""
        rr = RetrievalResults()
        if scoredocs is not None:
            for i in xrange(len(scoredocs)):
                score = scoredocs[i].score
                lucene_doc_id = scoredocs[i].doc  # internal doc_id
                doc_id = self.reader.document(lucene_doc_id).get(field_id)
                rr.append(doc_id, score, lucene_doc_id)
        return rr


    def __to_retrieval_groups(self, results, query_type, results_relation, rr):
        """Converts Lucene scoreDocs results to RetrievalResults format."""
        #rr = RetrievalResults()
        print "\tTotal hits:", results.totalHitCount, "groups:", len(results.groups), " and total groups:", results.totalGroupCount    
        if results.groups is not None:
            groups_num = len(results.groups)
            for i in xrange(groups_num):
                scoredocs = results.groups[i].scoreDocs
                #print 'group size...', results.groups[i].totalHits
                scoredocs_len = len(scoredocs)
                score = results.groups[i].maxScore# * float(scoredocs_len)
                mid = None
                docs = []
                for j in xrange(scoredocs_len):
                    lucene_doc_id = scoredocs[j].doc  # internal doc_id
                    #self.print_document(doc_id,term_vect=True)
                    docs.append(lucene_doc_id)
                    if not mid:
                        doc = self.reader.document(lucene_doc_id)
                        #print doc.getValues('text_sent')
                        mid = doc.getValues('mid')[0]
                        wikititle = None
                        wiki = doc.getValues('en_wikipedia')
                        if wiki:
                            wikititle = wiki[0]
                            
                if query_type == 'entity':
                    #print mid
                    if mid in results_relation.entities_dict:
                        #print "MID IN RELATIONS RESULTS"
                        rr.append_entity(mid, docs, score, wikititle, None)
                elif query_type == 'relation':
                    rr.append_relation(mid, docs, score, wikititle, None)
        return rr



    def score_query(self, query, field_content=FIELDNAME_TEXT, field_id=FIELDNAME_ID, num_docs=100):
        #Scores a given query and return results as a RetrievalScores object.
        lucene_query = self.get_lucene_query(query, field_content)
        scoredocs = self.searcher.search(lucene_query, num_docs).scoreDocs
        return self.__to_retrieval_results(scoredocs, field_id)


    def grouping_score_query(self, query,  query_type, results_relation, field_content=FIELDNAME_TEXT, field_id=FIELDNAME_ID, num_docs=100):
        grouping_search = GroupingSearch(self.FIELDNAME_MID)
        grouping_search.setGroupSort(Sort.RELEVANCE)
        #grouping_search.setCachingInMB(500.0, True)
        #grouping_search.setAllGroups(True)
        grouping_search.setGroupDocsLimit(num_docs)
        lucene_query = self.get_lucene_query(query, field_content)
        #print 'Lucene Query:', lucene_query, type(lucene_query)
        rr = RetrievalResults()
        results = grouping_search.search(self.searcher, lucene_query, 0, num_docs)
        rr = self.__to_retrieval_groups(results, query_type, results_relation, rr)
        #return self.__to_retrieval_groups(results, query_type, results_relation)
        print 'Number of groups retrieved:',len(rr.scores)
        return rr

    def num_docs(self):
        """Returns number of documents in the index."""
        self.open_reader()
        return self.reader.numDocs()

    def num_fields(self):
        """Returns number of fields in the index."""
        self.open_reader()
        atomic_reader = SlowCompositeReaderWrapper.wrap(self.reader)
        return atomic_reader.getFieldInfos().size()

    def get_fields(self):
        """Returns name of fields in the index."""
        fields = []
        self.open_reader()
        atomic_reader = SlowCompositeReaderWrapper.wrap(self.reader)
        for fieldInfo in atomic_reader.getFieldInfos().iterator():
            fields.append(fieldInfo.name)
        return fields

    def get_doc_termvector(self, lucene_doc_id, field):
        """Outputs the document term vector as a generator."""
        terms = self.reader.getTermVector(lucene_doc_id, field)
        if terms:
            termenum = terms.iterator(None)
            for bytesref in BytesRefIterator.cast_(termenum):
                yield bytesref.utf8ToString(), termenum

    def get_doc_termfreqs(self, lucene_doc_id, field):
        """
        Returns term frequencies for a given document field.

        :param lucene_doc_id: Lucene document ID
        :param field: document field
        :return dict: with terms
        """
        termfreqs = {}
        for term, termenum in self.get_doc_termvector(lucene_doc_id, field):
            termfreqs[term] = int(termenum.totalTermFreq())
        return termfreqs


    def get_group_termfreqs(self, group_docs, qterms):
        """
        Returns term frequencies for a given document field.

        :param lucene_doc_id: Lucene document ID
        :param field: document field
        :return dict: with terms
        """
        termfreqs = {}
        for lucene_doc_id in group_docs:
            #print "get_group_termfreqs ", lucene_doc_id
            count = len(qterms)
            for term, termenum in self.get_doc_termvector(lucene_doc_id, self.FIELDNAME_TEXT):
                if term in qterms:
                    count -= 1
                    try:
                        termfreqs[term] += int(termenum.totalTermFreq())
                    except KeyError:
                        termfreqs[term] = int(termenum.totalTermFreq())
                    if count == 0:
                        break
        return termfreqs


    def get_doc_termfreqs_all_fields(self, lucene_doc_id):
        """
        Returns term frequency for all fields in the given document.

        :param lucene_doc_id: Lucene document ID
        :return: dictionary {field: {term: freq, ...}, ...}
        """
        doc_termfreqs = {}
        vectors = self.reader.getTermVectors(lucene_doc_id)
        if vectors:
            for field in vectors.iterator():
                doc_termfreqs[field] = {}
                terms = vectors.terms(field)
                if terms:
                    termenum = terms.iterator(None)
                    for bytesref in BytesRefIterator.cast_(termenum):
                        doc_termfreqs[field][bytesref.utf8ToString()] = int(termenum.totalTermFreq())
                    print doc_termfreqs[field]
        return doc_termfreqs

    def get_coll_termvector(self, field):
        """ Returns collection term vector for the given field."""
        self.open_reader()
        fields = MultiFields.getFields(self.reader)
        if fields is not None:
            terms = fields.terms(field)
            if terms:
                termenum = terms.iterator(None)
                for bytesref in BytesRefIterator.cast_(termenum):
                    yield bytesref.utf8ToString(), termenum

    def get_coll_termfreq(self, term, field):
        """ from org.apache.lucene.analysis.miscellaneous import LimitTokenCountAnalyzer
        Returns collection term frequency for the given field.

        :param term: string
        :param field: string, document field
        :return: int
        """
        self.open_reader()
        return self.reader.totalTermFreq(Term(field, term))

    def get_doc_freq(self, term, field):
        """
        Returns document frequency for the given term and field.

        :param term: string, term
        :param field: string, document field
        :return: int
        """
        self.open_reader()
        return self.reader.docFreq(Term(field, term))

    def get_doc_count(self, field):
        """
        Returns number of documents with at least one term for the given field.

        :param field: string, field name
        :return: int
        """
        self.open_reader()
        return self.reader.getDocCount(field)

    def get_coll_length(self, field):
        """ 
        Returns length of field in the collection.

        :param field: string, field name
        :return: int
        """
        self.open_reader()
        return self.reader.getSumTotalTermFreq(field)

    def get_avg_len(self, field):
        """ 
        Returns average length of a field in the collection.

        :param field: string, field name
        """
        self.open_reader()
        n = self.reader.getDocCount(field)  # number of documents with at least one term for this field
        len_all = self.reader.getSumTotalTermFreq(field)
        if n == 0:
            return 0
        else:
            return len_all / float(n)

    def get_entity_avg_len(self, docs_len_dict):
        """
        returnt average doc length from the dictionary docs_len
        """
        return sum(docs_len_dict[d] for d in docs_len_dict) / float(len(docs_len_dict))
        
class LuceneEntityDocument(object):
    """Internal representation of a Lucene document."""

    def __init__(self):
        self.ldf = LuceneEntityDocumentField()

    def create_document(self, contents):
        """Create a Lucene document from the specified contents.
        Contents is a list of fields to be indexed, represented as a dictionary
        with keys 'field_name', 'field_type', and 'field_value'."""
        #print "CreateDOcument.-----------------------"
        doc = Document()
        for f in contents:
            doc.add(Field(f['field_name'], f['field_value'],
            self.ldf.get_field(f['field_type'])))
        return doc


class LuceneEntityDocumentField(object):
    """Internal handler class for possible field types."""

    def __init__(self):
        """Init possible field types."""

        # FIELD_ID: stored, indexed, non-tokenized
        self.field_id = FieldType()
        self.field_id.setIndexed(True)
        self.field_id.setStored(True)
        self.field_id.setTokenized(False)

        # FIELD_DOCID: stored, indexed, non-tokenized
        self.field_docid = FieldType()
        self.field_docid.setIndexed(True)
        self.field_docid.setStored(True)
        self.field_docid.setTokenized(False)

        # FIELD_WIKI_TITLE: stored, indexed, non-tokenized
        self.field_wiki_title = FieldType()
        self.field_wiki_title.setIndexed(True)
        self.field_wiki_title.setStored(True)
        self.field_wiki_title.setTokenized(False)

        # FIELD_MID: stored, indexed, non-tokenized
        self.field_mid = FieldType()
        self.field_mid.setIndexed(True)
        self.field_mid.setStored(True)
        self.field_mid.setTokenized(False)

        # FIELD_ID_TV: stored, indexed, not tokenized, with term vectors (without positions)
        # for storing IDs with term vector info
        self.field_id_tv = FieldType()
        self.field_id_tv.setIndexed(True)
        self.field_id_tv.setStored(True)
        self.field_id_tv.setTokenized(False)
        self.field_id_tv.setStoreTermVectors(True)

        # FIELD_TEXT: stored, indexed, tokenized, with positions
        self.field_text = FieldType()
        self.field_text.setIndexed(True)
        self.field_text.setStored(True)
        self.field_text.setTokenized(True)

        # FIELD_TEXT_TV: stored, indexed, tokenized, with term vectors (without positions)
        self.field_text_tv = FieldType()
        self.field_text_tv.setIndexed(True)
        self.field_text_tv.setStored(True)
        self.field_text_tv.setTokenized(True)
        self.field_text_tv.setStoreTermVectors(True)

        # FIELD_TEXT_TVP: stored, indexed, tokenized, with term vectors and positions
        # (but no character offsets)
        self.field_text_tvp = FieldType()
        self.field_text_tvp.setIndexed(True)
        self.field_text_tvp.setStored(True)
        self.field_text_tvp.setTokenized(True)
        self.field_text_tvp.setStoreTermVectors(True)
        self.field_text_tvp.setStoreTermVectorPositions(True)

        # FIELD_TEXT_NTV:  not stored, indexed, tokenized, with term vectors (without positions)
        self.field_text_ntv = FieldType()
        self.field_text_ntv.setIndexed(True)
        self.field_text_ntv.setStored(False)
        self.field_text_ntv.setTokenized(True)
        self.field_text_ntv.setStoreTermVectors(True)

        # FIELD_TEXT_TVP: not stored, indexed, tokenized, with term vectors and positions
        # (but no character offsets)
        self.field_text_ntvp = FieldType()
        self.field_text_ntvp.setIndexed(True)
        self.field_text_ntvp.setStored(False)
        self.field_text_ntvp.setTokenized(True)
        self.field_text_ntvp.setStoreTermVectors(True)
        self.field_text_ntvp.setStoreTermVectorPositions(True)

    def get_field(self, type):
        """Gets Lucene FieldType object for the corresponding internal FIELDTYPE_ value."""
        if type == Lucene.FIELDTYPE_ID:
            return self.field_id
        elif type == Lucene.FIELDTYPE_ID_TV:
            return self.field_id_tv
        elif type == Lucene.FIELDTYPE_TEXT:
            return self.field_text
        elif type == Lucene.FIELDTYPE_TEXT_TV:
            return self.field_text_tv
        elif type == Lucene.FIELDTYPE_TEXT_TVP:
            return self.field_text_tvp
        elif type == Lucene.FIELDTYPE_TEXT_NTV:
            return self.field_text_ntv
        elif type == Lucene.FIELDTYPE_TEXT_NTVP:
            return self.field_text_ntvp
        else:
            raise Exception("Unknown field type")
            

id
