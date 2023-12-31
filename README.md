# MIRCV-project

This project deals with the creation of a search engine through which a user can request information by using queries that return a list of documents. The relevance of the documents is computed basing on score determined with different metrics. The main parts of the project are:

- Index Building

- Query Processing

- Performance Evaluation


# Index Building

To generate the index, the file "collection.tsv", available at https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020, must be placed inside InvertedIndex/src/main/resources. The index building starts executing the IndexConstruction.java file in the path InvertedIndex/src/java/it/unipi/mircv. The program launch with default parameters:

- compression = true

- skipping list = true

- query = conjunctive

- score = MaxScore

- score mode = TFIDF

After the construction of the index, the following files are saved into src/main/resources/final:

- dictionary

- mergerd index blocks

- document table

- skip info

All these files are used in the query phase.


# Query Processing

Executing the file Main.java, after the index building, the user can modify the parameters and execute the queries:

- Conjunctive or disjunctive queries

- DAAT or MaxScore algorithm

- TFIDF or BM25

- the number of documents to obtain

Subsequently the top k documents are returned.


# Performance Evaluation

To evaluate the performance of the system, the msmarco-test2020-queries.tsv, that can be downloaded from https://microsoft.github.io/msmarco/TREC-Deep-Learning-2020 must be added to src/main/resources. The performance test starts with the execution of the file QueryTesting.java in QueryTesting/src/main/java/it/unipi/mircv/test.
