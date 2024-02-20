## OrientDB

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

------

## What is OrientDB?

**OrientDB** is an Open Source Multi-Model [NoSQL](http://en.wikipedia.org/wiki/NoSQL) DBMS with the support of Native Graphs, 
Documents, Full-Text search, Reactivity, Geo-Spatial and Object Oriented concepts. 
It's written in Java and it's amazingly fast. 
No expensive run-time JOINs, connections are managed as persistent pointers between records. 
You can traverse thousands of records in no time. Supports schema-less, schema-full and schema-mixed modes.
Has a strong security profiling system based on user, roles and predicate security and 
supports [SQL](https://orientdb.org/docs/3.1.x/sql/) amongst the query languages. Thanks to the [SQL](https://orientdb.org/docs/3.1.x/sql/) layer 
it's straightforward to use for people skilled in the Relational world.

[Get started with OrientDB](http://orientdb.org/docs/3.2.x/gettingstarted/) | [OrientDB Community Group](https://github.com/laa/orientdb/discussions).

## Is OrientDB a Relational DBMS?

No. OrientDB adheres to the [NoSQL](http://en.wikipedia.org/wiki/NoSQL) movement even though it supports [ACID Transactions](https://orientdb.org/docs/3.2.x/internals/Transactions.html) 
and [SQL](https://orientdb.org/docs/3.2.x/sql/) as query language. 
In this way it's easy to start using it without having to learn too much new stuff. 

## Easy to install and use

Yes. OrientDB is totally written in [Java](http://en.wikipedia.org/wiki/Java_%28programming_language%29) and can run on any platform without configuration and installation.
The full Server distribution is a few MBs without the demo database. Do you develop with a language different than Java? 
No problem, look at the [Programming Language Binding](http://orientdb.org/docs/3.1.x/apis-and-drivers/).


## Main References
- For any questions visit the [OrientDB Community Group](https://github.com/laa/orientdb/discussions)

[Get started with OrientDB](http://orientdb.org/docs/3.2.x/gettingstarted/).

--------
## Contributing

For the guide to contributing to OrientDB checkout the [CONTRIBUTING.MD](https://github.com/orientechnologies/orientdb/blob/develop/CONTRIBUTING.md)

All the contribution are considered licensed under Apache-2 license if not stated otherwise.

--------

## Licensing
OrientDB is licensed by OrientDB LTD under the Apache 2 license. 

--------


### Reference

Recent architecture re-factoring and improvements are described in our [BICOD 2021](http://ceur-ws.org/Vol-3163/BICOD21_paper_3.pdf) paper:

```
@inproceedings{DBLP:conf/bncod/0001DLT21,
  author    = {Daniel Ritter and
               Luigi Dell'Aquila and
               Andrii Lomakin and
               Emanuele Tagliaferri},
  title     = {OrientDB: {A} NoSQL, Open Source {MMDMS}},
  booktitle = {Proceedings of the The British International Conference on Databases
               2021, London, United Kingdom, March 28, 2022},
  series    = {{CEUR} Workshop Proceedings},
  volume    = {3163},
  pages     = {10--19},
  publisher = {CEUR-WS.org},
  year      = {2021}
}
```

