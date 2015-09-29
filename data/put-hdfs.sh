#!/bin/bash

script_path=$(dirname "${BASH_SOURCE[0]}")

hdfs dfs -put ${script_path}/companies.csv data
hdfs dfs -ls data

