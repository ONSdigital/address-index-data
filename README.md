# address-index-data 

[![Build Status](https://travis-ci.com/ONSdigital/address-index-data.svg?token=wrHpQMWmwL6kpsdmycnz&branch=develop)](https://travis-ci.com/ONSdigital/address-index-data)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/83c0fb7ca2e64567b0998848ca781a36)](https://www.codacy.com/app/Valtech-ONS/address-index-data?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=ONSdigital/address-index-data&amp;utm_campaign=Badge_Grade)
[![codecov](https://codecov.io/gh/ONSdigital/address-index-data/branch/develop/graph/badge.svg)](https://codecov.io/gh/ONSdigital/address-index-data)



### Purpose

This repository contains the Scala code for an Apache Spark job to create an Elasticsearch index from the AddressBase premium product.

For testing purposes there is a free [AddressBase sample](https://www.ordnancesurvey.co.uk/forms/builder/addressbase-premium-sample-data/20171011154036/frame) available from Ordnance Survey.

### Software and Versions

* Java 8 
* SBT 0.13.16 (http://www.scala-sbt.org/)
* Scala 2.12.4
* Apache Spark 2.3.2
* Elasticsearch 7.3.1

### Development Setup (IntelliJ)

* File, New, Project From Version Control, GitHub
* Git Repository URL - select https://github.com/ONSdigital/address-index-data
* Parent Directory: any local drive, typically your IdeaProjects directory
* Directory Name: address-index-data
* Clone

### Running

To package the project in a runnable fat-jar:
From the root of the project

```shell
sbt clean assembly
```

The resulting jar will be located in `batch/target/scala-2.10/ons-ai-batch-assembly-version.jar`

To run the jar:

```shell
java -Dconfig.file=application.conf -jar batch/target/scala-2.10/ons-ai-batch-assembly-version.jar
```
The target Elasticsearch index can be on a local ES deployment or an external server (configurable)
The `application.conf` file may contain:

```
addressindex.elasticsearch.nodes="just-the-hostname.com"
addressindex.elasticsearch.pass="password"
```

These will override the default configuration. The location and names of the input files can also be overridden.
Note that these input files are extracted from AddressBase and subject to some pre-processing.

The job can also be run from inside IntelliJ. 
In this case you can run the Main class directly but need to remove lines 40-118 and replace them with:
```
val indexName = generateIndexName(historical=true, skinny=true, nisra=true)
val url = s"http://$nodes:$port/$indexName"
postMapping(indexName, skinny=true)
saveHybridAddresses(historical=true, skinny=true, nisra=true)
```
where the first boolean is for a historic index, second for a skinny index and third to include Northern Ireland extract 

## Running Tests

Before you can run tests on this project if using Windows you must
  
  * Install the 64-bit version of winutils.exe https://github.com/steveloughran/winutils/raw/master/hadoop-2.6.0/bin/winutils.exe
  * save it on your local system in a bin directory e.g. c:\hadoop\bin
  * create environment variables HADOOP_HOME = c:\hadoop and hadoop.home.dir = c:\hadoop\bin
  * Update Path to add %HADOOP_HOME%\bin
  * Make temp directory writeable, on command line: winutils chmod 777 C:\tmp\hive (or other location of hive directory)
  * Now in IntelliJ you can mark the test directory (right-click, Mark Directory as, Test Resources Root).

Then next time you right-click the green arrow "Run ScalaTests" should be shown.

Note that you can't run the tests using sbt on the command line.

### Dependencies

Top level project dependencies may be found in the build.sbt file.
