# Xpark
 Python implementation of Apache Spark

## Usage
### RDD
```python
>>> import xpark
>>> ctx = xpark.Context()
>>> rdd = ctx.parallelize([{'i': i} for i in range(10)]).toRDD()

>>> rdd1 = rdd.collect()
>>> list(rdd1.execute())
[{'i': 0},
 {'i': 1},
 {'i': 2},
 {'i': 3},
 {'i': 4},
 {'i': 5},
 {'i': 6},
 {'i': 7},
 {'i': 8},
 {'i': 9}]

>>> rdd2 = rdd.map(lambda x: x['i'])\
              .filter(lambda i: i > 2)\
              .map(lambda i: (i % 2 == 0, i))\
              .groupByKey()\
              .collect()
>>> list(rdd2.execute())
[(False, [9, 7, 3, 5]), (True, [6, 8, 4])]
```

### Dataframe
```python
>>> import xpark
>>> ctx=xpark.Context()
>>> df = ctx.parallelize([
...     {'x': 1, 'y': 1, 'z': 1},
...     {'x': 2, 'y': 2, 'z': 2},
... ]).toDF()

>>> df = df.withColumn('w', df['y'] + df['z'], int)
>>> df = df.withColumn('w', df['w'] * 2, int)
>>> df = df.collect()
>>> list(df.execute())
[{'x': 1, 'y': 1, 'z': 1, 'w': 4}, {'x': 2, 'y': 2, 'z': 2, 'w': 8}]
```
