# from google.cloud import bigquery
# schema =  {'fields': [
#     {'name': 'insertId', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
#     {'name': 'session_id', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'msg', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'user', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'company', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'patientId', 'type': 'STRING', 'mode': 'NULLABLE'},
#     {'name': 'value', 'type': 'STRING', 'mode': 'NULLABLE'},
# ]}

schema = '''insertId:STRING,
          timestamp:STRING,
          session_id:STRING,
          msg:STRING,
          user:STRING,
          company:STRING,
          patientId:STRING,
          value:STRING'''