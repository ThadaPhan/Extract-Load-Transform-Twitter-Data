# ***Extract Load Transform Twitter Data***

## ***INTRODUCTION***
* Project stream data from [Twitter API](https://developer.twitter.com/en/docs/twitter-api), load to `MySql` and transform by `Spark`.
* Version <table>
    <tr>
        <td>Twitter API</td>
        <td>2</td>
    </tr>
    <tr>
        <td>Spark</td>
        <td>3.0.3</td>
    </tr>
    <tr>
        <td>Hadoop</td>
        <td>2.2.7</td>
    </tr>
    <tr>
        <td>Python</td>
        <td>3.7</td>
    </tr> 
    <tr>
        <td>Scala</td>
        <td>2.12.10</td>
    </tr>
    <tr>
        <td>Docker</td>
        <td>20.10.5</td>
    </tr>
   </table>


## ***INSTALLATION***

* You can install this project in [Extract Load Transform Twitter Data](https://github.com/ThadaPhan/Extract-Load-Transform-Twitter-Data.git)

## ***BUILD***

* Build Spark jar file before build docker image:
  * `cd Sentiment_Analysis`
  * `sbt-assembly`
* Copy Spark jar file to new place: `cp ./target/scala-2.12/Sentiment_Analysis-assembly-0.2.0.jar ../docker/spark/SparkJob`.
* Create *hadoop-network* with command: `docker network create --driver bridge spark-network --subnet=172.16.0.0/16`.
* Then, you go into the project and run command to build the hadoop cluster: `docker-compose build`.
* Final, you run: `docker-compose up -d`.
* Check result every 30 minutes in [HDFS UI](http://localhost:50070).

