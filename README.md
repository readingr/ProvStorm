ProvStorm
========================

This is a sample project for seeing if it is possible to use Storm in conjunction with provenance. This project is based off https://github.com/davidkiss/storm-twitter-word-count, and uses Storm version 0.8.1.

This projects creates an XML stream of provenance, which outputs a random amount of wasDerivedFrom relationships, and at random intervals includes a relationship we're interested in. Storm starts off with a static entity we're looking for, and monitors the stream for these interested relationships. When derivations are found from the initial entity, it is added into an ArrayList, which is then used for searching.  

This does not take into account transitive closures, it assumes the stream is ordered and consistent.

To get started:
* Clone/download this repository
* Import as existing Maven project in Eclipse
* Run ProvStream.java
* Run ProvTopology.java


The test concludes once it's found  "e6", the test can be modified easily by:

*Adding prov to search for in ProvStream.java
*Changing the random variable in ProvStream.java
*Changing the initial variable in ProvSpout.java
