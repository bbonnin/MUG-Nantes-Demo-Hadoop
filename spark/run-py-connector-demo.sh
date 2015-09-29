#!/bin/bash

script_path=$(dirname "${BASH_SOURCE[0]}")
mongo_libs=${script_path}/../mongodb/libs

spark-submit --jars ${mongo_libs}/mongo-hadoop-core-1.4.0.jar,${mongo_libs}/mongo-java-driver-3.0.3.jar ${script_path}/python/connector-demo.py

