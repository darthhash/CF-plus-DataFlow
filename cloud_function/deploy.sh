#!/bin/bash

py-cloud-fn newline-json bucket -p --python_version 3.5 -f main.py && \
cd cloudfn/target && gcloud beta functions deploy newline-json \
--trigger-bucket test-bardin22 --stage-bucket newline_cf --memory 2048MB && cd ../..