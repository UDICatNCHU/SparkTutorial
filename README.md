Apache Spark是一個開源叢集運算框架，最初是由加州大學柏克萊分校AMPLab所開發。相對於Hadoop的MapReduce會在執行完工作後將中介資料存放到磁碟中，Spark使用了記憶體內運算技術，能在資料尚未寫入硬碟時即在記憶體內分析運算。Spark在記憶體內執行程式的運算速度能做到比Hadoop MapReduce的運算速度快上100倍。


Some References :
* [http://www.mccarroll.net/blog/pyspark/index.html]
* [https://www.codementor.io/spark/tutorial/spark-python-rdd-basics]
* [http://backtobazics.com/big-data/spark/apache-spark-map-example/]

RDD為Apache Spark最核心之概念，有別於MapRduce，僅提供Map()與Reduce()兩項操作。
RDD提供兩大類別Transformation與Action

![see](http://feisky.xyz/pages/images/spark-transformation-list.png)

=======
# SparkTutorial
