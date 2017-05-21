# A Research Framework and Test Collection for Entity-Relationship Retrieval






## Tabular Data and Entity Relationships
Information that satisfies complex E-R queries is likely to involve instances of entities and their relationships dispersed across Web documents. Sometimes such information is collected and published within a single document, such as a Wikipedia page. In such cases, traditional search engines can provide excellent search results without applying special E-R techniques or considering entity and relationship types. Indeed, the data collection, aggregation, and tabularization has been done by a Wikipedia editor. 

That also means that a tabular Wikipedia content, comprising various entities, can be considered as representing a specific information need, i.e., the need that motivated editors to create the page in the first place. Such content can, in fact, satisfy many different information needs. We focus on exploiting tabular data for exhaustive search for pre-specified E-R types. In order to specify E-R queries, we can use column headings as entity types. All the column entries are then relevance judgments for the entity query. Similarly, for a given pair of columns that correspond to distinct entities, we formulate the implied relationship. For example the pair $<$car, manufacturing plant$>$ could refer to ``is made in'' or ``is manufactured in'' relationships. The instances of entity pairs in the table then serve as evidence for the specific relationship. This can be generalized to more complex information needs that involve multiple entity types and relationships.

Automated creation of E-R queries from tabular content is an interesting research problem. For now we asked human editors to provide natural language and structured E-R queries for specific entity types. Once we collect sufficient amounts of data from human editors we will be able to automate the query creation process with machine learning techniques. For the RELink QC we compiled a set of 600 queries with E-R relevance judgments from Wikipedia lists that relate to 9 different topic areas.   



## Selection of Tables
Wikipedia contains a dynamic index "The Lists of lists of lists (http://en.wikipedia.org/wiki/List_of_lists_of_lists) which represents the root of a tree that spans curated lists of entities in various domains. We used a Wikipedia snapshot from October 2016 to traverse The Lists of lists of lists tree starting from the root page and following every hyperlink of type "List of"  and their children. This resulted in a collection of 95,569 list pages. While most of the pages contain tabular data, only 18,903 include tables with consistent column and raw structure. Wee restrict content extraction to wikitable HTML class that typically denotes data tables in Wikipedia. We ignore other types of tables such as infoboxes. 

In this first instance, we focus on \textit{relational tables}, i.e., the tables that have a key column, referring to the \textit{main} entity in the table  \cite{lehmberg2016large}. For instance, the "List of books about skepticism" contains a table "Books" with columns "Author", "Category" and "Title", among others. In this case, the key column is "Title" which contains titles of books about skepticism. We require that any relationship specified for the entity types in the table must contain the  "Title" type, i.e., involve the "Title" column. 

In order to detect key columns we created a Table Parser that uses the set of heuristics adopted by Lehmberg et al. , e.g., the ratio of unique cells in the column or text length. Once the key column is identified, the parser creates entity pairs consisting of the key column and one other column in the table. The content of the column cells then constitutes the set of relevant judgments for the relationship specified by the pair of entities. 

For the sake of simplicity we consider only those Wikipedia lists that contain a single relational table. Furthermore, our goal is to create queries that have verifiable entity and entity pair instances. Therefore, we selected only those relational tables for which the key column and at least one more column have cell content linked to Wikipedia articles.  

With these requirements, we collected 1795 tables. In the final step, we selected 600 tables by performing stratified sampling across semantic domains covered by Wikipedia lists. For each new table, we calcuated the Jaccard similarity scores between the title of the corresponding Wikipedia page and the titles of pages associated with tables already in the pool. By setting the maximum similarity threshold to 0.7 we obtained a set of 600 tables. 

The process of creating RELink queries involves two steps: (1) automatic selection of tables and columns within tables and (2) manual specification of information needs. For example,  in the table "Grammy Award for Album of the Year" the columns "winner", "work" were automatically selected to serve as entity types in the E-R query. The relationship among these entities is suggested by the title and we let a human annotator to formulate the query.   

The RELink query set was created by 6 annotators. We provided the annotators with access to the full table, metadata (e.g., table title or the first paragraph of the page) and entity pairs or triples to be used to specify the query. For each entity pair or triple the annotators created a natural language information need and an E-R query in the relational format.













