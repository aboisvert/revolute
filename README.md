Revolute: Scala-based Query Language for Hadoop
===============================================

Revolute aims to provide a rich SQL-like querying language (embedded DSL)
inspired by Apache Hive and Scala Query.

The main benefit over Apache Hive is the increased expressivity -- with Scala's
powerful abstraction and composition facilities at your disposal.

Revolute is essentialy "thin layer" on top of Cascading, which provides the
muscle and a simplified working model for big data processing, including (but
not limited to) deployment on top of Apache Hadoop, an engaged open-source
community, and an wide ecosystem of extensions (sinks, taps, serializers, etc).

### Status ###

Revolute is a work-in-progress.  It is underdoing heavy development and is not
considered stable at the moment.

### Building ###

You need Apache Buildr 1.4.x or higher.

    # compile, test and package .jars
    buildr package

### Examples ###

See examples and tests under ```src/test/scala```.

And in the [documentation](https://github.com/aboisvert/revolute/wiki).

### Dependencies ###

* Hadoop 0.20.2+
* Cascading 2.0-wip+

### Target platform ###

* Scala 2.9.1 + JDK 1.6

### License ###

Revolute is is licensed under the terms of the Apache Software License v2.0.
<http://www.apache.org/licenses/LICENSE-2.0.html>

