#!/usr/bin/env bash
spark-submit --master local[*] --deploy-mode client --conf spark.shuffle.service.enabled=true create_dataset.py