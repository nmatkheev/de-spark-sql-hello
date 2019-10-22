#### Spark SQL hello-world  

Important issues:  
* use jdk 1.8  
* sbt archetype (like holdens) is awkward, for students you can make your own   
* sbt is slower than maven+scala


How to run:  
* set JAVA_HOME to jdk1.8
* download Spark (2.3.4, for hadoop 2.7), set SPARK_HOME  
* add Spark path to bin  
* put the payload (boston dataset)

```
spark-submit --master local[*] --class ru.nmatkheev.boston_wrangler.Boston target/scala-2.11/boston_wrangler_2.11-0.0.1.jar crime.csv offense_codes.csv /Users/lancer/IdeaProjects/boston_wrangler
```