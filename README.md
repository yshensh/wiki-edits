# wiki-edits

## Overview
Flink project and runs a streaming analysis program on a Flink cluster.

All implementations follow this guide
[Monitoring the Wikipedia Edit Stream](https://ci.apache.org/projects/flink/flink-docs-release-1.0/quickstart/run_example_quickstart.html)


## Build and Run
build JAR file
```bash
mvn clean package
```

execute main class
```bash
mvn exec:java -Dexec.mainClass=wikiedits.WikipediaAnalysis
```
