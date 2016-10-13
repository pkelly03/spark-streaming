Spark elasticsearch playground
==============================

Pull down the project
```
git clone git@github.com:pkelly03/spark-streaming.git
```


## Elastic search

You will need to start elastic search
```
> elasticsearch
```

## build app

Download and install sbt http://www.scala-sbt.org/download.html
Download and install scala 2.10.6 - I tried with 2.11.*. It was working out too messy as the elastic search contained its own
spark dependencies so there was too many conflicts.

cd to spark-streaming

```
spark-streaming> sbt clean compile pack
```

```
spark-streaming> ./target/pack/bin/recommender-app
```

you should be presented with a screen similar to below:

```
+---+----------+-----+------+--------+-------+------+--------------+---------------+-----+
|abv|appearance|aroma|beerId|brewerId|overall|palate|   profileName|          style|taste|
+---+----------+-----+------+--------+-------+------+--------------+---------------+-----+
|7.7|       4.0|  4.5| 47986|   10325|    4.0|   4.0|johnmichaelsen|German Pilsener|  4.5|
|7.7|       4.0|  4.5| 47986|   10325|    4.0|   4.0|johnmichaelsen|German Pilsener|  4.5|
|7.7|       4.0|  4.5| 47986|   10325|    4.0|   4.0|johnmichaelsen|German Pilsener|  4.5|
|7.7|       4.0|  4.5| 47986|   10325|    4.0|   4.0|johnmichaelsen|German Pilsener|  4.5|
|7.7|       4.0|  4.5| 47986|   10325|    4.0|   4.0|johnmichaelsen|German Pilsener|  4.5|
|7.7|       4.0|  4.5| 47986|   10325|    4.0|   4.0|johnmichaelsen|German Pilsener|  4.5|
|7.7|       4.0|  4.5| 47986|   10325|    4.0|   4.0|johnmichaelsen|German Pilsener|  4.5|
|7.7|       4.0|  4.5| 47986|   10325|    4.0|   4.0|johnmichaelsen|German Pilsener|  4.5|
|7.7|       4.0|  4.5| 47986|   10325|    4.0|   4.0|johnmichaelsen|German Pilsener|  4.5|
|7.7|       4.0|  4.5| 47986|   10325|    4.0|   4.0|johnmichaelsen|German Pilsener|  4.5|
+---+----------+-----+------+--------+-------+------+--------------+---------------+-----+
```
