#!/usr/bin/env bash

CONFIG=/home/spiro/GitRepos/spark-mongodb-connector/src/config

MONGOD=/usr/bin/mongod

DBROOT=/mnt/data1/mongoinst/data
DB1=${DBROOT}/db1
DB2=${DBROOT}/db2

${MONGOD} --config ${CONFIG}/mongod1-linux.cfg