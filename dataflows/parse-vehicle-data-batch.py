import argparse
import apache_beam as beam
import csv
import json
import logging
import os

import numpy as np

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam import DoFn

# Define a function to parse the CSV lines into dictionaries
def parse_csv(line):
    try:
        row = next(csv.reader([line]))
        return {
            "frame": int(row[0]),
            "id": int(row[1]),
            "x": float(row[2]),
            "y": float(row[3]),
            "width": float(row[4]),
            "height": float(row[5]),
            "xVelocity": float(row[6]),
            "yVelocity": float(row[7]),
            "xAcceleration": float(row[8]),
            "yAcceleration": float(row[9]),
            "frontSightDistance": float(row[10]),
            "backSightDistance": float(row[11]),
            "dhw": float(row[12]),
            "thw": float(row[13]),
            "ttc": float(row[14]),
            "precedingXVelocity": float(row[15]),
            "precedingId": int(row[16]),
            "followingId": int(row[17]),
            "leftPrecedingId": int(row[18]),
            "leftAlongsideId": int(row[19]),
            "leftFollowingId": int(row[20]),
            "rightPrecedingId": int(row[21]),
            "rightAlongsideId": int(row[22]),
            "rightFollowingId": int(row[23]),
            "laneId": int(row[24])
        }
    except (ValueError, IndexError):
        # Return None for rows that do not conform to the schema
        return None

# Define a function to convert Python dictionaries to JSON strings
def to_json(element):
    return json.dumps(element)


def create_kv_pairs(element):
    return {
        "frame": element["frame"],
        "id": element["id"],
        "x": element["x"],
        "y": element["y"],
        "width": element["width"],
        "height": element["height"],
        "xVelocity": element["xVelocity"],
        "yVelocity": element["yVelocity"],
        "xAcceleration": element["xAcceleration"],
        "yAcceleration": element["yAcceleration"],
        "frontSightDistance": element["frontSightDistance"],
        "backSightDistance": element["backSightDistance"],
        "dhw": element["dhw"],
        "thw": element["thw"],
        "ttc": element["ttc"],
        "precedingXVelocity": element["precedingXVelocity"],
        "precedingId": element["precedingId"],
        "followingId": element["followingId"],
        "leftPrecedingId": element["leftPrecedingId"],
        "leftAlongsideId": element["leftAlongsideId"],
        "leftFollowingId": element["leftFollowingId"],
        "rightPrecedingId": element["rightPrecedingId"],
        "rightAlongsideId": element["rightAlongsideId"],
        "rightFollowingId": element["rightFollowingId"],
        "laneId": element["laneId"],
    }


import numpy as np

def calculate_vehicle_avgs(data):
    vehicle_id, values = data

    # Extract TTC values
    ttc_values = [entry['ttc'] for entry in values]
    
    # Calculate quartiles and IQR
    q1 = np.percentile(ttc_values, 25)
    q3 = np.percentile(ttc_values, 75)
    iqr = q3 - q1
    
    # Define outlier bounds
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr
    
    # Filter out outliers
    filtered_values = [entry for entry in values if entry['ttc'] >= lower_bound and entry['ttc'] <= upper_bound and entry['ttc'] >= 0]
    
    # Recalculate metrics using filtered values
    count = len(filtered_values)
    if count > 0:
        total_velocity = sum(entry['xVelocity'] for entry in filtered_values)
        total_acceleration = sum(entry['xAcceleration'] for entry in filtered_values)
        
        avg_velocity = total_velocity / count
        avg_acceleration = total_acceleration / count
        
        non_zero_ttc_values = [entry['ttc'] for entry in filtered_values if entry['ttc'] != 0]
        min_ttc = min(non_zero_ttc_values) if non_zero_ttc_values else 0
        max_ttc = max(non_zero_ttc_values) if non_zero_ttc_values else 0
    else:
        avg_velocity = 0
        avg_acceleration = 0
        min_ttc = 0
        max_ttc = 0

    return {
        'id': vehicle_id,
        'average_velocity': avg_velocity,
        'average_acceleration': avg_acceleration,
        'min_ttc': min_ttc,
        'max_ttc': max_ttc
    }

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input file to process.')
    parser.add_argument('--output_table', dest='output_table', required=True, help='Pub/Sub big query database to write results to.')
    parser.add_argument('--output_table2', dest='output_table2', required=True, help='Pub/Sub big query database to write results to.')

    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set up the pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True;

    # Define the pipeline
    with beam.Pipeline(options=pipeline_options) as p:

        # Read the input CSV file
        lines = p | 'Read from file' >> beam.io.ReadFromText(known_args.input)
        
        # Parse the CSV lines into dictionaries
        converted_data = lines | 'Parse CSV' >> beam.Map(parse_csv)

        # Remove error lines
        parsed_data = converted_data | 'Filter Errors' >> beam.Filter(lambda x: x is not None)


        # map parsed dat to kv pairs
        key_value_pairs = parsed_data | 'Create key-value pairs' >> beam.Map(create_kv_pairs)        


        # Define the schema for the BigQuery table
        schema = {
            'fields': [
                {'name': 'frame', 'type': 'INTEGER'},
                {'name': 'id', 'type': 'INTEGER'},
                {'name': 'x', 'type': 'FLOAT'},
                {'name': 'y', 'type': 'FLOAT'},
                {'name': 'width', 'type': 'FLOAT'},
                {'name': 'height', 'type': 'FLOAT'},
                {'name': 'xVelocity', 'type': 'FLOAT'},
                {'name': 'yVelocity', 'type': 'FLOAT'},
                {'name': 'xAcceleration', 'type': 'FLOAT'},
                {'name': 'yAcceleration', 'type': 'FLOAT'},
                {'name': 'frontSightDistance', 'type': 'FLOAT'},
                {'name': 'backSightDistance', 'type': 'FLOAT'},
                {'name': 'dhw', 'type': 'FLOAT'},
                {'name': 'thw', 'type': 'FLOAT'},
                {'name': 'ttc', 'type': 'FLOAT'},
                {'name': 'precedingXVelocity', 'type': 'FLOAT'},
                {'name': 'precedingId', 'type': 'INTEGER'},
                {'name': 'followingId', 'type': 'INTEGER'},
                {'name': 'leftPrecedingId', 'type': 'INTEGER'},
                {'name': 'leftAlongsideId', 'type': 'INTEGER'},
                {'name': 'leftFollowingId', 'type': 'INTEGER'},
                {'name': 'rightPrecedingId', 'type': 'INTEGER'},
                {'name': 'rightAlongsideId', 'type': 'INTEGER'},
                {'name': 'rightFollowingId', 'type': 'INTEGER'},
                {'name': 'laneId', 'type': 'INTEGER'}
            ]
        }

        # write to output table (Big Query)
        key_value_pairs | 'Write full data to BQ' >> beam.io.WriteToBigQuery(
            table=known_args.output_table,
            schema=schema, 
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

        # map parsed dat to kv pairs
        grouped_data = key_value_pairs | 'Map rows to vehicle ID' >> beam.Map(lambda x: (x['id'], x))  | 'Group by vehicle' >> beam.GroupByKey()       

        # calculate averages
        avg_data = grouped_data | 'Calculate avgs and filter' >> beam.Map(calculate_vehicle_avgs)

        # Define the schema for the BigQuery table
        schema2 = {
            'fields': [
                {'name': 'id', 'type': 'INTEGER'},
                {'name': 'average_velocity', 'type': 'FLOAT'},
                {'name': 'average_acceleration', 'type': 'FLOAT'},
                {'name': 'min_ttc', 'type': 'FLOAT'},
                {'name': 'max_ttc', 'type': 'FLOAT'}

            ]
        }

        # write to output table (Big Query)
        avg_data | 'Write vehicle average data' >> beam.io.WriteToBigQuery(
            table=known_args.output_table2,
            schema=schema2, 
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
