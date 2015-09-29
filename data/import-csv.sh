#!/bin/bash

script_path=$(dirname "${BASH_SOURCE[0]}")

mongoimport ${script_path}/msft.csv --type csv --headerline -d marketdata -c stock_prices

