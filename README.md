# address-index-data [![Build Status](https://travis-ci.com/ONSdigital/address-index-data.svg?token=wrHpQMWmwL6kpsdmycnz&branch=develop)](https://travis-ci.com/ONSdigital/address-index-data)

### Prerequisites

* Java 6 or higher
* SBT (http://www.scala-sbt.org/)

### Development Setup (MacOS)

Launch elasticsearch:2 either with docker or through dedicated `dev` vagrant machine

- `brew cask install docker`
- `docker run -d -p 9200:9200 -p 9300:9300 elasticsearch:2`

or

- `vagrant run dev`

### Running

TBD

To package the project in a runnable fat-jar:

```shell
sbt assembly
```

The resulting jar will be located in `batch/target/scala-2.10/ons-ai-batch-assembly-version.jar`

To run the jar:

```shell
java -Dconfig.file=application.conf -jar batch/target/scala-2.10/ons-ai-batch-assembly-version.jar
```

The `application.conf` file may contain:

```
addressindex.elasticsearch.nodes="just-the-hostname.com"
addressindex.elasticsearch.pass="password"
```

These will override the default configuration pointing to the localhost.

### Dependencies

Top level project dependencies may be found in the build.sbt file.
