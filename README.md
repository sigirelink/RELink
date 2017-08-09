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





## Selection of Tables
Wikipedia contains a dynamic index "The Lists of lists of lists (http://en.wikipedia.org/wiki/List_of_lists_of_lists) which represents the root of a tree that spans curated lists of entities in various domains. We used a Wikipedia snapshot from October 2016 to traverse The Lists of lists of lists tree starting from the root page and following every hyperlink of type "List of"  and their children. This resulted in a collection of 95,569 list pages. While most of the pages contain tabular data, only 18,903 include tables with consistent column and raw structure. Wee restrict content extraction to wikitable HTML class that typically denotes data tables in Wikipedia. We ignore other types of tables such as infoboxes. 

In this first instance, we focus on \textit{relational tables}, i.e., the tables that have a key column, referring to the \textit{main} entity in the table  \cite{lehmberg2016large}. For instance, the "List of books about skepticism" contains a table "Books" with columns "Author", "Category" and "Title", among others. In this case, the key column is "Title" which contains titles of books about skepticism. We require that any relationship specified for the entity types in the table must contain the  "Title" type, i.e., involve the "Title" column. 

In order to detect key columns we created a Table Parser that uses the set of heuristics adopted by Lehmberg et al. , e.g., the ratio of unique cells in the column or text length. Once the key column is identified, the parser creates entity pairs consisting of the key column and one other column in the table. The content of the column cells then constitutes the set of relevant judgments for the relationship specified by the pair of entities. 

For the sake of simplicity we consider only those Wikipedia lists that contain a single relational table. Furthermore, our goal is to create queries that have verifiable entity and entity pair instances. Therefore, we selected only those relational tables for which the key column and at least one more column have cell content linked to Wikipedia articles.  

With these requirements, we collected 1795 tables. In the final step, we selected 600 tables by performing stratified sampling across semantic domains covered by Wikipedia lists. For each new table, we calcuated the Jaccard similarity scores between the title of the corresponding Wikipedia page and the titles of pages associated with tables already in the pool. By setting the maximum similarity threshold to 0.7 we obtained a set of 600 tables. 

The process of creating RELink queries involves two steps: (1) automatic selection of tables and columns within tables and (2) manual specification of information needs. For example,  in the table "Grammy Award for Album of the Year" the columns "winner", "work" were automatically selected to serve as entity types in the E-R query. The relationship among these entities is suggested by the title and we let a human annotator to formulate the query.   

The RELink query set was created by 6 annotators. We provided the annotators with access to the full table, metadata (e.g., table title or the first paragraph of the page) and entity pairs or triples to be used to specify the query. For each entity pair or triple the annotators created a natural language information need and an E-R query in the relational format.


## Collection Details

RELink Query Collection covers 9 thematic areas from the "Lists-of-Lists-of-Lists" in Wikipedia: Mathematics and Logic, Religion and Belief Systems, Technology and Applied Sciences, Miscellaneous, People, Geography and Places, Natural and Physical Sciences, General Reference and Culture and the Arts. The most common thematic areas are Culture and the Arts with 70 queries and Geography and Places with 67 queries.

In Table \ref{stats} we show the characteristics of the natural language and relational queries. Among 600 E-R queries, 381 refer to entity pairs and 219 to entity triples. As expected, natural language descriptions of 3-entity queries are longer (on average 83.8 characters) compared to 2-entity queries (56.5 characters).

We further analyze the structure of relational queries and their components, i.e., entity queries that specify the entity type and relationship queries that specify the relationship type.  Across 600 queries, there are 1251 unique entity types (out of total 1419 occurrences). They are rather unique across queries: only 65 entity types occur in more than one E-R query and 44 occur in exactly 2 queries. The most commonly shared entity type is "country", present in 9 E-R queries.

In the case of relationships, there are 317 unique relationship types (out of 817 occurrences) with a dominant type "located in" that occurs in 140 queries. This is not surprising since in many domains the key entity is tied to a location that is included in one of the columns. Nevertheless, there are only 44 relationship types occurring more than once implying that RELink QC is a diverse set of queries, including 273 different relationship types. 









