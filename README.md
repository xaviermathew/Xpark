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


>>> df = ctx.List([
...     {'x': 1, 'y': 1, 'z': 1},
...     {'x': 2, 'y': 2, 'z': 2},
... ]).toDF()

>>> print(list(df['x'].execute()))
[1, 2]

>>> df['x'] = df['y'] + df['z']
>>> print(list(df['x'].execute()))
[2, 4]
>>> print(list((df['x'] * 2).execute()))
[4, 8]

>>> print(list(df.execute()))
[{'x': 2, 'y': 1, 'z': 1}, {'x': 4, 'y': 2, 'z': 2}]
```
