Apache Spark是一個開源叢集運算框架，最初是由加州大學柏克萊分校AMPLab所開發。相對於Hadoop的MapReduce會在執行完工作後將中介資料存放到磁碟中，Spark使用了記憶體內運算技術，能在資料尚未寫入硬碟時即在記憶體內分析運算。Spark在記憶體內執行程式的運算速度能做到比Hadoop MapReduce的運算速度快上100倍。


Reference:
* [http://www.mccarroll.net/blog/pyspark/index.html]
* [https://www.codementor.io/spark/tutorial/spark-python-rdd-basics]


RDD為Apache Spark最核心之概念，有別於MapRduce，僅提供Map()與Reduce()兩項操作。
RDD提供兩大類別Transformation與Action

![see](http://feisky.xyz/pages/images/spark-transformation-list.png)

### WordCount


```python
import urllib
f=urllib.urlretrieve("https://www.ccel.org/ccel/bible/kjv.txt","bible")
```


```python
print f
```

    ('bible', <httplib.HTTPMessage instance at 0x7f8a1c32ea28>)



```python
data_file = "./bible"
```


```python
print data_file
```

    ./bible



```python
text_file = sc.textFile(data_file)
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.take(5)
```




    [(u'', 322787),
     (u'Iru,', 1),
     (u'formed,', 2),
     (u'Dioscorinthius.', 1),
     (u'shouted,', 6)]



#### word count sortByKey example


```python
text_file = sc.textFile(data_file)
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts = counts.sortByKey()
counts.take(5)
```




    [(u'', 322787), (u'"A', 1), (u'"Ama', 1), (u'"As', 3), (u'"Both', 1)]



#### word count sortBy


```python
text_file = sc.textFile(data_file)
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts = counts.sortBy(lambda x: x[1], ascending=False)
counts.take(5)
```




    [(u'', 322787),
     (u'the', 72271),
     (u'and', 46446),
     (u'of', 40346),
     (u'to', 16539)]



#### map 與 flatMap的差異


```python
text_file = sc.textFile(data_file)
counts = text_file.map(lambda line: line.split(" "))
counts.take(5)
```




    [[u'',
      u'',
      u'',
      u'',
      u'',
      u'__________________________________________________________________'],
     [u''],
     [u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'Title:',
      u'The',
      u'King',
      u'James',
      u'Version',
      u'of',
      u'the',
      u'Holy',
      u'Bible'],
     [u'', u'', u'', u'', u'', u'', u'Creator(s):', u'Anonymous'],
     [u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'',
      u'Rights:',
      u'Public',
      u'Domain']]




```python

```




    [(u'', 322787),
     (u'Iru,', 1),
     (u'formed,', 2),
     (u'Dioscorinthius.', 1),
     (u'shouted,', 6)]



### pi-estimation 


```python
import random

def sample(p):
    x, y = random.random(), random.random()
    return 1 if x*x + y*y < 1 else 0

count = sc.parallelize(xrange(0, 100000)).map(sample) \
             .reduce(lambda a, b: a + b)
print "Pi is roughly %f" % (4.0 * count / 100000)
```

    Pi is roughly 3.146200



```python
count = sc.parallelize(xrange(0, 1000000)).map(lambda p: 1 if (random.random()**2 + random.random()**2)<1 else 0) \
             .reduce(lambda a, b: a + b)
print "Pi is roughly %f" % (4.0 * count / 1000000)
```

    Pi is roughly 3.143024


### Text Search Example


```python
import urllib
f=urllib.urlretrieve("https://www.ccel.org/ccel/bible/kjv.txt","bible")
text_file = sc.textFile(data_file)
lines = text_file.map(lambda line: line) 
lines.take(20)
```




    [u'     __________________________________________________________________',
     u'',
     u'           Title: The King James Version of the Holy Bible',
     u'      Creator(s): Anonymous',
     u'          Rights: Public Domain',
     u'   CCEL Subjects: All; Bible; Old Testament; New Testament; Apocrypha',
     u'      LC Call no: BS185',
     u'     LC Subjects:',
     u'',
     u'                  The Bible',
     u'',
     u'                  Modern texts and versions',
     u'',
     u'                  English',
     u'     __________________________________________________________________',
     u'',
     u'Holy Bible',
     u'',
     u'                               King James Version',
     u'     __________________________________________________________________']



### Filter Example


```python
import urllib
f=urllib.urlretrieve("https://www.ccel.org/ccel/bible/kjv.txt","bible")
text_file = sc.textFile(data_file)
lines = text_file.filter(lambda line: 'and' in line) 
lines.take(20)
```




    [u'                  Modern texts and versions',
     u'   Great and manifold were the blessings, most dread Sovereign, which',
     u"   England, when first he sent Your Majesty's Royal Person to rule and",
     u'   Occindental Star, Queen Elizabeth, of most happy memory, some thick and',
     u'   palpable clouds of darkness would so have overshadowed this land, that',
     u'   men should have been in doubt which way they were to walk, and that it',
     u'   dispelled those supposed and surmised mists, and gave unto all that',
     u'   beheld the Government established in Your Highness, and Your hopeful',
     u'   Seed, by an undoubted Title; and this also accompanied with peace and',
     u'   tranquillity at home and abroad.',
     u'   only to the time spent in this transitory world, but directeth and',
     u'   and to continue it in that state wherein the famous Predecessor of Your',
     u'   Highness did leave it; nay, to go forward with the confidence and',
     u'   resolution of a man, in maintaining the truth of Christ, and',
     u'   propagating it far and near is that which hath so bound and firmly knit',
     u"   the hearts of all Your Majesty's loyal and religious people unto You,",
     u'   with comfort, and they bless You in their hearts, as that sanctified',
     u'   every day increaseth and taketh strength, when they observe that the',
     u'   backward, but is more and more kindled, manifesting itself abroad in',
     u'   healed,) and every day at home, by religious and learned discourse, by']




```python

```
