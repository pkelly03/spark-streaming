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


Other Notes

need equivalent for this in mac - this is command for linux
```
ulimit -n 65536 
```


### Scala Notebook Jupyter

Here is a [link to the scala notebook Jupyter](https://github.com/alexarchambault/jupyter-scala)

```
pip3 install jupyter
```

To execute the notebooks, go to directory where notebooks are

```
jupyter notebook
```

### Elastic Search notes

List all indices
```
curl 'localhost:9200/_cat/indices?v'
```


## Importing data into elastic search

Install elasticdump

```
npm install -g elasticdump
```

Import the mappings first:
```
elasticdump --input=./mapping_ba:item.json --output=localhost:9200/ba:items --type=mapping 
elasticdump --input=./mapping_ba:users.json --output=localhost:9200/ba:users --type=mapping 
elasticdump --input=./mapping_ba:rec_related.json --output=localhost:9200/ba:rec_tarelated --type=mapping 
```

Import the data next:
```
elasticdump --input=http://production.es.com:9200/my_index --output=/data/my_index_mapping.json --type=mapping 
elasticdump --input=http://production.es.com:9200/my_index --output=/data/my_index_mapping.json --type=mapping 
elasticdump --input=http://production.es.com:9200/my_index --output=/data/my_index_mapping.json --type=mapping 
```

## Notebooks

```
cd /Users/paukelly/Dropbox/Msc/beer-recommender/notebooks
jupyter notebook
```

## Install UI ElasticHQ

Install on mac by:
```
/usr/local/Cellar/elasticsearch/2.4.1/libexec/bin/plugin install royrusso/elasticsearch-HQ 
```

Go to viewer and play around:
```
http://localhost:9200/_plugin/hq
```


## Elastic Search curl commands

Good reference:
https://www.safaribooksonline.com/library/view/elasticsearch-cookbook-/9781783554836/ch05s02.html

```
curl -XGET 'http://localhost:9200/ba:users/ba:users/_search?q=_id:barnaclebill'


```

# Breeze nlp/scalala 
https://github.com/scalanlp/breeze/wiki/Quickstart
