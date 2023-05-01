from google.cloud import storage
import json
storage_client = storage.Client()

def hello_gcs_generic(data,context):
    try:
        bucket = storage_client.get_bucket(data['bucket'])
        blob = bucket.blob(data['name'])
        contents = blob.download_as_string()
        result = [json.dumps(record) for record in
                    json.loads(contents)]

        x = '\n'.join(result)
        bucket = storage_client.get_bucket('test-bardin22')
        blob = bucket.blob('{} newline.json'.format(data['name'].replace('.json','')))
        blob.upload_from_string(x)
    except Exception as e:
        print(e)