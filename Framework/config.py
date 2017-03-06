from os import path

RSDM_DIR = path.dirname(path.abspath(__file__))
OUTPUT_DIR = path.dirname(path.abspath(__file__)) + "/results"
INDEX_DIR = ""
ENTITY_INDEX_DIR = ""
ENT_DOCS_LEN = ""
RELATION_INDEX_DIR = ""
ENT_REL_DOCS_FREQ = ""
REL_DOCS_LEN = ""
print "Entity index:", ENTITY_INDEX_DIR
print "Relation index:", RELATION_INDEX_DIR
