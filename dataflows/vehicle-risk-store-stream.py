import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging
import json

def run(argv=None, save_main_session=True):    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', dest='input', type=str, help='Input Pub/Sub topic name', required=True)
    parser.add_argument('--output_table', dest='output', type=str, help='Output BigQuery table', required=True)
    parser.add_argument('--risk_tolerance', dest='tolerance', type=float, help='Tolerance for risk to filter', required=True)  
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(streaming=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
            | 'Read from topic' >> beam.io.ReadFromPubSub(topic=known_args.input)
            | "Unpack msg json" >> beam.Map(lambda x: json.loads(x))
            | "Filter by risk tolerance" >> beam.Filter(lambda x: x['risk'] > known_args.tolerance)
            | "Prepare data for BigQuery" >> beam.Map(lambda x: 
                {'id': x['id'],
                 'risk': x['risk'],
                 'avg_vel':x['average_velocity'],
                 'min_ttc':x['min_ttc']
                 })  
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table=known_args.output,
                schema='id:STRING,risk:FLOAT,avg_vel:FLOAT,min_ttc:FLOAT', 
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
            )
        )

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
