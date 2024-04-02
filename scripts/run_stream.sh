#!/bin/bash

source set_env_variables.sh

python ../dataflows/vehicle-data-processing-stream.py \
    --input_topic=projects/$PROJECT/topics/vehicle_data  \
    --output_topic=projects/$PROJECT/topics/risk_analysis  \
    --project $PROJECT \
    --region northamerica-northeast2 \
    --runner DataflowRunner \
    --temp_location $BUCKET/tmp/ \
    --streaming 
