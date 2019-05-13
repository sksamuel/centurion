rxhive
=======

[![Build Status](https://travis-ci.org/sksamuel/rxhive.svg?branch=master)](https://travis-ci.org/sksamuel/rxhive)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel/rxhive*.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Ca%3A%22rxhive)
[<img src="https://img.shields.io/nexus/s/https/oss.sonatype.org/com.sksamuel.rxhive/rxhive-core.svg?label=latest%20snapshot&style=plastic"/>](https://oss.sonatype.org/content/repositories/snapshots/com/sksamuel/rxhive/)
![License](https://img.shields.io/github/license/sksamuel/rxhive.svg?style=plastic)

Coming soon
-----------


Processing Steps for writing struct data to hive:

Presteps:

* If the table exists and overwrite mode is used, delete the table
* If the table does not exist and overwrite/create mode is used, create the table from the supplied schema or derive it from the first record. Partition columns must be specified

For each struct:

* Resolve the schema in the struct with the schema in the metastore using a SchemaResolver.
* If the table is partitioned:
    * Calculate the partition for this struct
    * Using a partitioner: locate the partition write directory and ensure that the partition exists in the metastore and on the filesystem
* If the table is not partitioned:
    * Return the base table location
* Calculate the write schema from the input schema, the metastore schema, and the partitions (if any)
* Derive an updated struct using the incoming struct and the write schema
* Get or create a writer for the write location
    * If the writer has not been created, create a new file using the file manager.
* Write the aligned struct using a struct writer

After all structs:

* Close each writer to flush any data
* For each file, complete the file via the file manager.
