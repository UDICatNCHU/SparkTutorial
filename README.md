
#Spark's RDD operations.

This is the data set used for The Third International Knowledge Discovery and Data Mining Tools Competition, which was held in conjunction with KDD-99 The Fifth International Conference on Knowledge Discovery and Data Mining. The competition task was to build a network intrusion detector, a predictive model capable of distinguishing between ``bad'' connections, called intrusions or attacks, and ``good'' normal connections. This database contains a standard set of data to be audited, which includes a wide variety of intrusions simulated in a military network environment.



Example:

```
  val data = Array(1, 2, 3, 4, 5)ddd
  val distData = sc.parallelize(data)
```

## Some References
1. [https://www.codementor.io/spark/tutorial/spark-python-rdd-basics]
2. [http://www.mccarroll.net/blog/pyspark/index.html]


```python
array = [1,10,20]
```


```python
print array
```

    [1, 10, 20]



```python
array.extend([30,25])
```


```python
print array

```

    [1, 10, 20, 30, 25]



```python
print sorted(array)
```

    [1, 10, 20, 25, 30]



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
```


```python
print counts
```

```
import random
def sample(p):
    x, y = random.random(), random.random()
    return 1 if x*x + y*y < 1 else 0

count = sc.parallelize(xrange(0, 100000)).map(sample) \
             .reduce(lambda a, b: a + b)
print "Pi is roughly %f" % (4.0 * count / 100000)
```
另一種寫法

```
count = sc.parallelize(xrange(0, 1000000)).map(lambda p: 1 if (random.random()**2 + random.random()**2)<1 else 0) \
             .reduce(lambda a, b: a + b)
print "Pi is roughly %f" % (4.0 * count / 1000000)
```
