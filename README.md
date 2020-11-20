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

>>> rdd2 = rdd.filter(lambda x: x['i'] > 2)\
              .map(lambda x: (x['i'] % 2 == 0, x))\
              .groupByKey()\
              .collect()
>>> list(rdd2.execute())
[(True,
  [{'i': 10}, {'i': 8}, {'i': 4}, {'i': 6}, {'i': 6}, {'i': 8}, {'i': 4}]),
 (False,
  [{'i': 7}, {'i': 9}, {'i': 5}, {'i': 3}, {'i': 9}, {'i': 7}, {'i': 3}, {'i': 5}])]
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
