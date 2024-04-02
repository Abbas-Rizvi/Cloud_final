#!/bin/bash

source set_env_variables.sh


python ../dataflows/parse-vehicle-data-batch.py \
    --input=$BUCKET/data/01_tracks.csv \
    --output_table=$PROJECT.highway_analysis_db.full_vehicle_data  \
    --output_table2=$PROJECT.highway_analysis_db.vehicle_data_averages  \
    --temp_location $BUCKET/temp \
    --region=northamerica-northeast2 \
    --runner=DataflowRunner \
    --project=$PROJECT

