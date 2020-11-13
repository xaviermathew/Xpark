# Xpark
 Python implementation of Apache Spark

## Usage
```python
>>> import xpark
>>> ctx = xpark.Context()
>>> p = ctx.parallelize(range(10)).collect()

>>> list(p.execute())
[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]

>>> p = ctx.parallelize(range(10)).map(lambda x: x + 1).filter(lambda x: x > 2).collect()
>>> list(p.execute())
[3, 4, 5, 6, 7, 8, 9, 10]

>>> p = ctx.parallelize(range(10)).map(lambda x: (x % 2 == 0, x)).groupByKey().collect()
>>> list(p.execute())
[(False, [9, 7, 3, 5, 1]), (True, [6, 8, 4, 0, 2])]


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
