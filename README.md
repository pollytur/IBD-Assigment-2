# Introduction To Big Data Assignment 2

## Project description
This project analyzies a stream of Twitter messages (aka tweets) and determine their sentiment as either negative or positive according to the pretrained model. Implemented using Scala language and Apache Spark framework and includes next stages:

1. **Streaming** - the `Streamer` is responsible for connecting to the remote stream and providing next layers an `DSSteam` object to work with  
2. **Preprocessing** - remove user aliases, links, transforms the whole tweet to lower cases and decrypts abbreviations. Also remove most common words to fasten word2vec.
3. **Feature extraction** - tokenization and vectorization with the help of Word2Vec.
4. **Training** - Train model on provided dataset with one of the next models - `Gradient Boosting Tree` and `Logistric Regresion`.
5. **Predicting** - fed into model tweets obtained from the stream, predict sentiment. Then, write predictions to the output file.

## Structure
- **model**
    - **Train** - Train on dataset and save the model to file 
- **preprocessing**
    - **ReplaceDictionaries** - decrypts abbreviations
    - **StopWords** - remove most common words
    - **TweetPreprocess** - Provide interface for preprocessing
- **streaming**
    - **Streamer** - Process stream and return tweets
- **test**
    - **TweetPreprocessSpec** - unit test for preprocessing
- **Main** - Take result from streamer and save output csv with predictions 

## How to run the project

1. run in the project folder
>sbt package

2. copy target/scala-2.12/project-2_2.12-0.1.jar to cluster

3. copy myGNTree myLR myWord2Vec to hdfs to your user repo

4. run on cluster
> HADOOP_CONF_DIR=/hadoop/etc/hadoop /spark-3.0.1-bin-hadoop2.7/bin/spark-submit --master yarn project-2_2.12-0.1.jar

## [Link to full report](https://hackmd.io/PWzJJy3cSWiIVeA2-PBuSA)