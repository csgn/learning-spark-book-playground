SPARK_HOME=/home/csgn/Playground/learning-spark/spark3

$SPARK_HOME/bin/spark-submit \
    --class chapter2.MnMCount \
    target/scala-2.12/mysources_2.12-1.0.jar \
    data/mnm_dataset.csv

