import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import argparse
import logging
import json

def calculate_risk(element):
    vehicle_id = element['id']  # Assuming 'id' field corresponds to 'vehicle_id'
    velocity = element['average_velocity']
    acceleration = element['average_acceleration']
    
    risk = abs(velocity / element['min_ttc'])
        
    output_data = {
        'id':vehicle_id,
        'risk': float(risk),
        'average_velocity':abs(velocity),
        'min_ttc':element['min_ttc']
        }
    return output_data

def run(argv=None, save_main_session=True):    
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_topic', dest='input', type=str, help='Input Pub/Sub topic name', required=True)
    parser.add_argument('--output_topic', dest='output', type=str, help='Output Pub/Sub topic name', required=True)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(streaming=True)

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (pipeline
            | 'Read from topic' >> beam.io.ReadFromPubSub(topic=known_args.input)
            | "toDict" >> beam.Map(lambda x: json.loads(x))
            | 'Filter Non-Zero Values' >> beam.Filter(lambda x: x['max_ttc'] != 0 or x['min_ttc'] != 0)
            | 'Calculate Risk Percentage' >> beam.Map(calculate_risk)  
            | 'to byte' >> beam.Map(lambda x: json.dumps(x).encode('utf8'))
            | 'to Pub/sub' >> beam.io.WriteToPubSub(topic=known_args.output)
        )

if __name__ == '__main__':
    # logging.getLogger().setLevel(logging.INFO)
    run()
