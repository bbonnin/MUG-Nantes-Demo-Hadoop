#!/bin/bash

script_path=$(dirname "${BASH_SOURCE[0]}")
demo_jar=${script_path}/java/target/demo-hadoop-connector-0.0.1-SNAPSHOT-jar-with-dependencies.jar

spark-submit --class "mug.DemoHadoopConnector" --master "local[2]" ${demo_jar}

