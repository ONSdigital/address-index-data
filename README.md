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

### Dependencies

Top level project dependencies may be found in the build.sbt file.
