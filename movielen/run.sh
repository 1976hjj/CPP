#!/usr/bin/env bash
spark-submit --master local[*] --deploy-mode client --conf spark.shuffle.service.enabled=true --driver-memory 8G processall.py --interval 10
