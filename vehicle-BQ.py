import argparse
import apache_beam as beam
import csv
import json
import logging
import os

from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


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

def calculate_avg_velocity_acceleration(data):
    vehicle_id, values = data

    total_velocity = sum(entry['xVelocity'] for entry in values)
    total_acceleration = sum(entry['xAcceleration'] for entry in values)

    count = len(values)

    avg_velocity = total_velocity / count if count > 0 else 0
    avg_acceleration = total_acceleration / count if count > 0 else 0

    return {
        'id': vehicle_id,
        'average_velocity': avg_velocity,
        'average_acceleration': avg_acceleration
    }

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', dest='input', required=True, help='Input file to process.')
    parser.add_argument('--output', dest='output', required=True, help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    # Set up the pipeline options
    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True;

    # Define the pipeline
    with beam.Pipeline(options=pipeline_options) as p:

        # read the input CSV file
        lines = p | 'ReadFromText' >> beam.io.ReadFromText(known_args.input)
        
        # parse the CSV lines into dictionaries
        converted_data = lines | 'ParseCSV' >> beam.Map(parse_csv)

        # remove error lines
        parsed_data = converted_data | 'FilterErrors' >> beam.Filter(lambda x: x is not None)

        # map parsed dat to kv pairs
        key_value_pairs = parsed_data | 'ExtractKey' >> beam.Map(lambda x: (x['id'], x))        

        # group by vehicle
        grouped_data = key_value_pairs | 'GroupByVehicle' >> beam.GroupByKey()
        
        # calculate averages
        avg_data = grouped_data | 'CalculateAverage' >> beam.Map(calculate_avg_velocity_acceleration)
        
        # Define the schema for the BigQuery table
        schema = {
            'fields': [
                {'name': 'id', 'type': 'INTEGER'},
                {'name': 'average_velocity', 'type': 'FLOAT'},
                {'name': 'average_acceleration', 'type': 'FLOAT'}
            ]
        }

        # write to output table (Big Query)
        avg_data | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
            table=known_args.output,
            schema=schema, 
            method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
