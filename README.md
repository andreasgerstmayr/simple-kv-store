simple kv store
===============

very simple distributed key/value storage using the [vert.x application framework](http://vertx.io), implemented in python

Features
--------

* in-memory key/value store (supported operations: GET, PUT, DELETE, ALLKEYS)
* distributed
* sharding

Startup
-------

    vertx runmod at.andreasgerstmayr~simple-kv-store~1.0 -conf conf.json

Configuration
-------------

Take a look at ```conf.json```
