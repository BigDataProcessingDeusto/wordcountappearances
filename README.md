# WordCountAppearance

This exercise performs classical word count task from Apache Hadoop MapReduce, plus calculating the distribution of appearances and sorting the result.

## Instructions

To compile the code:

```
$ mvn compile
```

To create a jar package:
```
$ mvn package
```

To execute the code:
```
$ yarn jar target/wordcountappearance-1.0-SNAPSHOT.jar es.deusto.bdp.hadoop.wordcountappearance.WordCountAppearance input/ output-job1/ output-job2/ output-job3/
```
