
#Spark's RDD operations.

This is the data set used for The Third International Knowledge Discovery and Data Mining Tools Competition, which was held in conjunction with KDD-99 The Fifth International Conference on Knowledge Discovery and Data Mining. The competition task was to build a network intrusion detector, a predictive model capable of distinguishing between ``bad'' connections, called intrusions or attacks, and ``good'' normal connections. This database contains a standard set of data to be audited, which includes a wide variety of intrusions simulated in a military network environment.



Example:
  val data = Array(1, 2, 3, 4, 5)ddd
  val distData = sc.parallelize(data)

## Some References
[Reference][https://www.codementor.io/spark/tutorial/spark-python-rdd-basics]
[Reference][http://www.mccarroll.net/blog/pyspark/index.html]


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

    PythonRDD[6] at RDD at PythonRDD.scala:4
     (u'^35Lod,', 1),
     (u'fruits:', 2),
     (u'warm.', 1),
     (u'excellency', 24),
     (u'Shebarim,', 1),
     (u'escaped?', 1),
     (u'adulteries', 1),
     (u'unders
 
     (u'contemned', 1),
     (u'mirth,', 11),
     (u'^20Hell', 1),
     (u'drams;', 1),
     (u'Olivet,', 2),
     (u'locked', 1),
     (u'^55Now', 1),
     (u'^20Seeing', 1),
     (u'pursue', 29),
     (u'terrified,', 1),
     (u'condescend', 1),
     (u'kettle,', 1),
     (u'wept;', 3),
     (u'laughter;', 1),
     (u'flocks?', 2),
     (u'redeemed:', 3),
     (u'flocks;', 5),
     (u'^6May', 1),
     (u'decay,', 1),
     (u'Cain,', 8),
     (u'^29Zadok', 1),
     (u'choosest', 1),
     (u'Perizzites:', 1),
     (u'^4With', 2),
     (u'^17Remember', 1),
     (u'^11Phinehas,', 1),
     (u'Zephath,', 1),
     (u'perfection:', 2),
     (u'household', 19),
     (u'Bernice,', 2),
     (u'Babel,', 1),
     (u'earthquake:', 2),
     (u'malign', 2),
     (u'kingdoms', 43),
     (u'want', 28),
     (u'fathers', 252),
     (u'^15Were', 2),
     (u'drunkards.', 1),
     (u'Add', 1),
     (u'reprove', 21),
     (u'pilgrims,', 1),
     (u'Melchisedec.', 4),
     (u'Casleu.', 2),
     (u'youth;', 8),
     (u'drunkards,', 3),
     (u'rites;', 1),
     (u'keep,', 8),
     (u'husband?', 3),
     (u'travel', 5),
     (u'^78And', 2),
     (u'fetters', 7),
     (u'^19His', 1),
     (u'^26At', 2),
     (u'soothsayer,', 1),
     (u'hot', 21),
     (u'Titus', 7),
     (u'embalmed:', 1),
     (u'^3Hearken;', 1),
     (u'front', 2),
     (u'^16Thus', 12),
     (u'PrAzarias', 3),
     (u'reproached:', 1),
     (u'Nebuchadrezzar', 29),
     (u'keep:', 1),
     (u'A', 76),
     (u'renounced', 1),
     (u'names.', 5),
     (u'beauty', 48),
     (u'skill;', 1),
     (u'fingers:', 1),
     (u'grew:', 2),
     (u'affrighted;', 1),
     (u'^6For', 76),
     (u'^98Thou', 1),
     (u'^18Is', 3),
     (u'reproachfully.', 1),
     (u'wrong', 14),
     (u'Dictators', 1),
     (u'^3Before', 1),
     (u'^82Then', 1),
     (u'^73Now', 1),
     (u'worshippeth', 4),
     (u'prayers,', 11),
     (u'dough', 4),
     (u'grew,', 12),
     (u'promotion', 2),
     (u'grew.', 2),
     (u'Anak.', 2),
     (u'powder.', 2),
     (u'Anak,', 5),
     (u'calves', 13),
     (u'prayers.', 1),
     (u'Egypt;)', 1),
     (u'contentment', 2),
     (u'ho,', 1),
     (u'revolt', 3),
     (u'powder,', 4),
     (u'^12Remember,', 1),
     (u'consolations', 1),
     (u'Pispah,', 1),
     (u'brightly,', 1),
     (u'^2Fulfil', 1),
     (u'^35Women', 1),
     (u'glorious?', 1),
     (u'"To', 1),
     (u'^30So', 8),
     (u'satyrs', 1),
     (u'wind', 80),
     (u'company?', 2),
     (u'rubbish;', 1),
     (u'Even', 19),
     (u'nourished', 16),
     (u'effect.', 5),
     (u'whore;', 1),
     (u'effect,', 3),
     (u'rewarder', 1),
     
     (u'whip', 2),
     (u'inward:', 1),
     (u'Mattathah,', 1),
     (u'forward;', 4),
     (u'prescribed,', 1),


