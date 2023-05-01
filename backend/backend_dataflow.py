from apache_beam.options.pipeline_options import PipelineOptions
import os
import apache_beam as beam
import argparse
import json
import backend_config
import backend_schema
from apache_beam.coders import Coder

parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg', help='description')
args, beam_args = parser.parse_known_args()

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = backend_config.google_service_key
input_file = backend_config.input_file.replace('.json', '') + ' newline.json'
print(type(input_file))
project = backend_config.project
output_dataset = backend_config.output_dataset
schema = backend_schema.schema
runner = backend_config.runner
job_name = backend_config.job_name
temp_location = backend_config.temp_location
region = backend_config.region

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
        session_id = data['jsonPayload']['message']['session_id']
        msg = data['jsonPayload']['message']['msg']
        user = data['jsonPayload']['message']['data'].get('user')
        company = data['jsonPayload']['message']['data'].get('company')
        patientId = data['jsonPayload']['message']['data'].get('patientId')
        value = data['jsonPayload']['message']['data'].get('value')
        print(value)
        print({'insertId':insertId,'timestamp':timestamp,'session_id':session_id,'msg':msg,'user':user,'company':company,'patientId':patientId,'value':value})
        return [{'insertId':insertId,'timestamp':timestamp,'session_id':session_id,'msg':msg,'user':user,'company':company,'patientId':patientId,'value':value}]


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
                            "{0}:{1}.backend_logs".format(str(project), output_dataset),
                            schema = schema,
                            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
                      )

