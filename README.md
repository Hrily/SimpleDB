# SimpleDB

**SimpleDB** is a java based basic Database Management System. This project focuses on implementing core modules required for a DBMS which includes modules required to store and retrieve data on disk, transaction, locking and executing queries.

The project is course project for **CS186** course of **Berkeley University**. The full details can be obtained at the project site :
    https://sites.google.com/site/cs186fall2013/homeworks/project-1
    
### Documentation

You can find entire JavaDoc at [https://hrily.github.io/SimpleDB](https://hrily.github.io/SimpleDB) 
### Features
+ Modelling of DB Tables in form of *Tuples* and *Fields*
+ Modelling of DB *Catalog*
+ Management of Buffer Pages in *BufferPool*
+ Arranging Pages in *HeapFiles* and retrieving data from them
+ DB Table Scan using *Iterators*
+ *Filter* Operator
+ *Join* Operator
+ *Aggregator (Group by)* Operator
+ *Insertion* and *Deletion* into Tables
+ *Page Eviction* in Buffer Pool
+ *Join Optimizer* based on Selinger cost-based optimizer
+ *Transaction, Locking and Concurrency control*

### TODO List
- [ ] B+Tree Files
- [x] Better Thread Scheduling
- [ ] Better Jonn Optimizer Cost computing

The project is done over skeleton provided by the course. Testing is done on JUnit using the tests provided by course itself.
