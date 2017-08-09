# A Research Framework and Test Collection for Entity-Relationship Retrieval






**Entity-Relationship (E-R) Retrieval:** given a query containing types of multiple entities and relationships connecting them, search for relevant tuples of related entities.

**Example:** *Silicon Valley companies founded by Harvard graduates* expects a list of tuples *<company, founder>* as results.

**Problem:** lack of test collections for E-R retrieval.

**Contributions:**
1. A low-effort semi-automatic method for acquiring instances of entities and entity relationships from tabular data.
1. RELink Query Collection of 600 E-R queries with corresponding relevance judgments.
1. RELink Framework with resources that enable experimentation with multi-relationship E-R queries.



We prepared and released the RELink Query Collection comprising 600 Entity-Relationship queries and relevance judgments based on a sample of Wikipedia "List-of-lists-of-lists" tables.

The process of creating RELink queries involves two steps: 
1. automatic selection of tables and columns within tables
2. manual specification of information needs.


## Tabular Data and Entity Relationships
Information that satisfies complex E-R queries is likely to involve instances of entities and their relationships dispersed across Web documents. 

Tabular Wikipedia content, comprising various entities, can be considered as representing a specific information need.
For a given pair of columns that correspond to distinct entities, we formulate the implied relationship.

The instances of entity pairs in the table then serve as evidence, i.e., relevance judgments for the specific relationship.


## Query Generation

1. *The Lists of lists of lists* represents the root of a tree that spans curated Wikipedia lists of entities in various domains including 18,903 tables with consistent column and row structure.

2.  We focus on relational tables, i.e., the tables that have a key column, referring to the main entity in the table

3. Once the key column is identified, the parser creates entity pairs consisting of the key column and one other column in the table. 

4. We ended up with 1795 tables. In the final step, we selected 600 tables by performing stratified sampling across semantic domains covered by Wikipedia lists.

5. We provided the annotators with access to the full table, metadata and entity pairs/triples.

6. For each entity pair/triple the annotators created a E-R query in both natural language and in the relational format Q = { Q^{E_i}, Q^{R_{i,j}}, Q^{E_j} }.



## Collection Details

RELink Query Collection covers 9 thematic areas from the *Lists-of-Lists-of-Lists* in Wikipedia: Mathematics and Logic, Religion and Belief Systems, Technology and Applied Sciences, Miscellaneous, People, Geography and Places, Natural and Physical Sciences, General Reference and Culture and the Arts. The most common thematic areas are Culture and the Arts with 70 queries and Geography and Places with 67 queries.

Among 600 E-R queries, 381 refer to entity pairs and 219 to entity triples. As expected, natural language descriptions of 3-entity queries are longer (on average 83.8 characters) compared to 2-entity queries (56.5 characters).

We further analyze the structure of relational queries and their components, i.e., entity queries that specify the entity type and relationship queries that specify the relationship type.  Across 600 queries, there are 1251 unique entity types (out of total 1419 occurrences). They are rather unique across queries: only 65 entity types occur in more than one E-R query and 44 occur in exactly 2 queries. The most commonly shared entity type is "country", present in 9 E-R queries.

In the case of relationships, there are 317 unique relationship types (out of 817 occurrences) with a dominant type "located in" that occurs in 140 queries. This is not surprising since in many domains the key entity is tied to a location that is included in one of the columns. Nevertheless, there are only 44 relationship types occurring more than once implying that RELink QC is a diverse set of queries, including 273 different relationship types. 



## SIGIR PAPER

We have a resource paper describing RELInk published at SIGIR 2017.

You can find the paper pdf [here](https://arxiv.org/pdf/1706.03960.pdf)

If you use RELink, please use the following citation:

@inproceedings{Saleiro:2017:RRF:3077136.3080756,
 author = {Saleiro, Pedro and Milic-Frayling, Natasa and Mendes Rodrigues, Eduarda and Soares, Carlos},
 title = {RELink: A Research Framework and Test Collection for Entity-Relationship Retrieval},
 booktitle = {Proceedings of the 40th International ACM SIGIR Conference on Research and Development in Information Retrieval},
 series = {SIGIR '17},
 year = {2017},
 location = {Shinjuku, Tokyo, Japan},
 pages = {1273--1276},
 doi = {10.1145/3077136.3080756},
 publisher = {ACM}
} 







