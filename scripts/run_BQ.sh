#!/bin/bash

source set_env_variables.sh


python vehicle-BQ.py \
    --input=$BUCKET/data/01_tracks.csv \
    --output=$PROJECT.highway_analysis_db.Test2  \
    --temp_location $BUCKET/temp \