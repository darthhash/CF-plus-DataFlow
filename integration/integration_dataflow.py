from apache_beam.options.pipeline_options import PipelineOptions
import os
import apache_beam as beam
import argparse
import json
import integration_config
import integration_schema
from apache_beam.coders import Coder

parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg', help='description')
args, beam_args = parser.parse_known_args()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = integration_config.google_service_key
input_file = integration_config.input_file.replace('.json', '') + ' newline.json'
print(type(input_file))
project = integration_config.project
output_dataset = integration_config.output_dataset
schema = integration_schema.schema
runner = integration_config.runner
job_name = integration_config.job_name
temp_location = integration_config.temp_location
region = integration_config.region

beam_options = PipelineOptions(
    beam_args,
    runner = runner,
    project = project,
    job_name = job_name,
    temp_location = temp_location,
    region = region
    # runner='DataflowRunner',
    # project='coastal-volt-385014',
    # job_name='unique-job-name11',
    # temp_location='gs://test-bardin22/temp',
    # region='asia-east2'
)

class JsonCoder(Coder):
  """A JSON coder interpreting each line as a JSON string."""
  def encode(self, x):
    return json.dumps(x).encode('utf-8')

  def decode(self, x):
    return json.loads(x)

class extract_logs_to_dict(beam.DoFn):
    def process(self, data):
        insertId = data['insertId']
        timestamp = data['timestamp']
        event = data['jsonPayload']['message']['event']
        user = data['jsonPayload']['message']['data']['user']
        company = data['jsonPayload']['message']['data']['company']
        patientId = data['jsonPayload']['message']['data']['patientId']

        print({'insertId':insertId,'timestamp':timestamp,'event':event,'user':user, 'company':company,'patientId':patientId})
        return [{'insertId':insertId,'timestamp':timestamp,'event':event,'user':user, 'company':company,'patientId':patientId}]


def print_row(row):
    print(row)
    print(type(row))

with beam.Pipeline(options=beam_options) as pipeline:
      data_backend = ( pipeline
                      | 'ReadMyFile' >> beam.io.ReadFromText(input_file, coder = JsonCoder())
                      # |'to json' >> beam.Map(lambda x: json.loads(json.dumps(x)))

                      |'Parse json to DataFrame' >> beam.ParDo(extract_logs_to_dict())
                      # |"Print" >> beam.Map(print_row)
                      |'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                            "{0}:{1}.integraion_logs".format(str(project), output_dataset),
                            schema = schema,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                      )

