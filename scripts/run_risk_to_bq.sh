#!/bin/bash

source set_env_variables.sh

python ../dataflows/vehicle-risk-store-stream.py \
    --input_topic=projects/$PROJECT/topics/risk_analysis  \
    --output_table=$PROJECT.highway_analysis_db.risk_results \
    --risk_tolerance=1 \
    --project $PROJECT \
    --region northamerica-northeast2 \
    --runner DataflowRunner \
    --temp_location $BUCKET/tmp/ \
    --streaming 
