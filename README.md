# A Research Framework and Test Collection for Entity-Relationship Retrieval




The resources here available are described in a resource paper under revision at SIGIR 2017. Further details about these resources will be provided once the resource paper is published.


## Tabular Data and Entity Relationships
Information that satisfies complex E-R queries is likely to involve instances of entities and their relationships dispersed across Web documents. Sometimes such information is collected and published within a single document, such as a Wikipedia page. In such cases, traditional search engines can provide excellent search results without applying special E-R techniques or considering entity and relationship types. Indeed, the data collection, aggregation, and tabularization has been done by a Wikipedia editor. 

That also means that a tabular Wikipedia content, comprising various entities, can be considered as representing a specific information need, i.e., the need that motivated editors to create the page in the first place. Such content can, in fact, satisfy many different information needs. We focus on exploiting tabular data for exhaustive search for pre-specified E-R types. In order to specify E-R queries, we can use column headings as entity types. All the column entries are then relevance judgments for the entity query. Similarly, for a given pair of columns that correspond to distinct entities, we formulate the implied relationship. For example the pair $<$car, manufacturing plant$>$ could refer to ``is made in'' or ``is manufactured in'' relationships. The instances of entity pairs in the table then serve as evidence for the specific relationship. This can be generalized to more complex information needs that involve multiple entity types and relationships.

Automated creation of E-R queries from tabular content is an interesting research problem. For now we asked human editors to provide natural language and structured E-R queries for specific entity types. Once we collect sufficient amounts of data from human editors we will be able to automate the query creation process with machine learning techniques. For the RELink QC we compiled a set of 600 queries with E-R relevance judgments from Wikipedia lists that relate to 9 different topic areas.   

















