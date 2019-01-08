# Bullet DSL

[![Build Status](https://travis-ci.org/bullet-db/bullet-dsl.svg?branch=master)](https://travis-ci.org/bullet-db/bullet-dsl) [![Coverage Status](https://coveralls.io/repos/github/bullet-db/bullet-dsl/badge.svg?branch=master)](https://coveralls.io/github/bullet-db/bullet-dsl?branch=master) [![Download](https://api.bintray.com/packages/yahoo/maven/bullet-dsl/images/download.svg) ](https://bintray.com/yahoo/maven/bullet-dsl/_latestVersion)

A DSL for users to plug in their datasource into Bullet (Spark, Storm, etc.)

## Table of Contents

- [Background](#background)
- [Install](#install)
- [Usage](#usage)
- [Documentation](#documentation)
- [Links](#links)
    - [Quick Links](#quick-links)
- [Contributing](#contributing)
- [License](#license)

## Background

Bullet is a streaming query engine that can be plugged into any singular data stream using a Stream Processing framework like Apache [Storm](https://storm.apache.org), [Spark](https://spark.apache.org) or [Flink](https://flink.apache.org). It lets you run queries on this data stream - including hard queries like Count Distincts, Top K etc. The main project is available **[here](https://github.com/bullet-db/bullet-core)**.

## Install

Bullet DSL is a library written in Java and published to [Bintray](https://bintray.com/yahoo/maven/bullet-dsl) and mirrored to [JCenter](http://jcenter.bintray.com/com/yahoo/bullet/bullet-dsl/). To see the various versions and set up your project for your package manager (Maven, Gradle etc), [see here](https://bullet-db.github.io/releases/#bullet-dsl).

## Usage

Bullet DSL consists of two major components: the BulletConnector and the BulletRecordConverter. The BulletConnector is used to read data (objects) from a pluggable datasource while the BulletRecordConverter
converts those objects into BulletRecords.

Bullet Storm and Spark (and others) will provide a reading component that will use BulletConnector and BulletRecordConverter, so users will not have to write code themselves but will instead provide configuration.

Below are some examples for users that wish to use BulletConnector or BulletRecordConverter separately.

#### BulletConnector

The currently implemented BulletConnectors are KafkaConnector and PulsarConnector which support Apache Kafka and Apache Pulsar respectively.

Example usage:

    BulletDSLConfig config = new BulletDSLConfig();
    BulletConnector connector = BulletConnector.from(config);
    try {
        connector.initialize();
    } catch (BulletDSLException e) {
        // handle exception
    }
    ...
    List<Object> messages;
    try {
        messages = connector.read();
    } catch (BulletDSLException e) {
        // handle exception
    }
    ...
    try {
        connector.close();
    } catch (Exception e) {
        // handle exception
    }

#### BulletRecordConverter

The currently implemented BulletRecordConverters are AvroBulletRecordConverter, MapBulletRecordConverter, and POJOBulletRecordConverter. These converters support converting
Apache Avro records, maps, and POJOs to BulletRecords.

Note, BulletRecordConverter can be used with or without a BulletRecordSchema; the schema can be specified in the configuration as a json file.

Example usage:

    BulletDSLConfig config = new BulletDSLConfig();
    BulletRecordConverter converter = BulletRecordConverter.from(config);
    try {
        BulletRecord record = converter.convert(object);
    } catch (BulletDSLException e) {
        // handle exception
    }

#### BulletRecordSchema

An array of objects where each object is a BulletRecordField that consists of a name, reference, type, and subtype.

When an object is converted, the name of the fields in the resulting BulletRecord are specified by the schema and the corresponding values by the corresponding references.
If a reference is null, the corresponding name will be used instead.

Possible types are: BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, STRING, LIST, LISTOFMAP, MAP, MAPOFMAP, and RECORD.

Possible subtypes are: BOOLEAN, INTEGER, LONG, FLOAT, DOUBLE, AND STRING.

Note, if type is MAP, MAPOFMAP, LIST, or LISTOFMAP, then a subtype is required (otherwise subtype must be null). If type is RECORD, then name should be left empty.

Example schema and fields:

    [
      {
        "name": "myBool",
        "type": "BOOLEAN"
      },
      {
        "name": "myBoolMap",
        "type": "MAP",
        "subtype": "BOOLEAN"
      },
      {
        "name": "myLongMapMap",
        "type": "MAPOFMAP",
        "subtype": "LONG"
      },
      {
        "name": "myIntFromSomeMap",
        "reference": "someMap.myInt",
        "type": "INTEGER"
      },
      {
        "name": "myIntFromSomeIntList",
        "reference": "someIntList.0",
        "type": "INTEGER"
      },
      {
        "name": "myIntFromSomeNestedMapsAndLists",
        "reference": "someMap.nestedMap.nestedList.0",
        "type": "INTEGER"
      },
      {
        "reference" : "someMap",
        "type": "RECORD"
      }
    ]

## Documentation

All documentation is available at **[Github Pages here](https://bullet-db.github.io/)**.

## Links

* [Bullet DSL](https://bullet-db.github.io/backend/dsl/) to see the complete DSL documentation.

### Useful Links

* [Spark Quick Start](https://bullet-db.github.io/quick-start/spark) to start with a Bullet instance running locally on Spark.
* [Storm Quick Start](https://bullet-db.github.io/quick-start/storm) to start with a Bullet instance running locally on Storm.
* [Spark Architecture](https://bullet-db.github.io/backend/spark-architecture/) to see how Bullet is implemented on Storm.
* [Storm Architecture](https://bullet-db.github.io/backend/storm-architecture/) to see how Bullet is implemented on Storm.
* [Setup on Spark](https://bullet-db.github.io/backend/spark-setup/) to see how to setup Bullet on Spark.
* [Setup on Storm](https://bullet-db.github.io/backend/storm-setup/) to see how to setup Bullet on Storm.
* [API Examples](https://bullet-db.github.io/ws/examples/) to see what kind of queries you can run on Bullet.
* [Setup Web Service](https://bullet-db.github.io/ws/setup/) to setup the Bullet Web Service.
* [Setup UI](https://bullet-db.github.io/ui/setup/) to setup the Bullet UI.

## Contributing

All contributions are welcomed! Feel free to submit PRs for bug fixes, improvements or anything else you like! Submit issues, ask questions using Github issues as normal and we will classify it accordingly. See [Contributing](Contributing.md) for a more in-depth policy. We just ask you to respect our [Code of Conduct](Code-of-Conduct.md) while you're here.

## License

Code licensed under the Apache 2 license. See the [LICENSE](LICENSE) for terms.
