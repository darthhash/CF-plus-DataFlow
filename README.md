# CF-plus-DataFlow
Repo contains code for ETL process extracting any new files from GCS and moving them to BQ by means of CF and ApacheBeam

## Introduction 
Firstly I wanted to create one-step solution using only GCP DataFlow options, but during the work I've met challenges, which made my job a little bit complicated. The problem was with input json file, its format is standart json, while ApacheBeam works only with newline-separated json (that allows to process job in parallel). So the solution presented here includes two step:
- **Cloud Function**

  which is triggered by any file upload to specified GCP bucket and create newline-separated json file.
- **DataFlow process**
  which reads the new file and loads it to BigQuery 
  

I know that this solution is not optimal, but I hope it better shows the possibilities of GCP platform. The core idea can be upgrdated - for example we can trigger PubSub topic on CloudFunction success, which in its turn will trigger DataFlow process. Of course it's all too tricky as for simple json loading - actually we can do that without any code at all, just by some SQL transformations. But if we imagine that we want to stream data in real-time, my idea, I hope, will help us. 

## Deploying Notes. Cloud Function

### From GCP UI
Firstly we should create CloudFunction whith **Event type** = 'On (finalizing/creating) file in the selected bucket' as on the pic below:

<img width="565" alt="Снимок экрана 2023-05-01 в 21 13 43" src="https://user-images.githubusercontent.com/62540074/235465090-ef14b004-ce6c-4e10-bcc3-c252711d1057.png">

Next we need to just add source code into Inline editor and change **bucket_name** to actual one: 

<img width="1196" alt="Снимок экрана 2023-05-01 в 21 32 31" src="https://user-images.githubusercontent.com/62540074/235468091-0cfda735-4420-4dff-b0ab-9f17508d5baf.png">

Also It can be done through GitHub Actions but process is too complicated 


## Deploying Notes. Apache Beam

Actually we can just change creds in config.py file and run command:

```
python backend_dataflow.py 
python integration_dataflow.py 
```
It can be scheduled with Google Cloud Scheduler

## Dashboard

It's here - https://lookerstudio.google.com/s/pyusREb15rQ

Based on ratio_mart.sql request, which is scheduled in BQ 

## The analytics case summing up
Basically all insights we can get from the data on picture:
<img width="1041" alt="Снимок экрана 2023-05-05 в 19 06 46" src="https://user-images.githubusercontent.com/62540074/236453522-9b7dba2d-74db-4764-a79e-8fecc2212fff.png">

We can see that the result for a new onboarding type is worse in terms of Voice Ratio metric by almost 3%, but the result here is not statistically significant. Confidence interval is 65.9% – 69% for old type and 59.8% – 68.8% for new one, so anyway it is biassed to the negative side for new onboarding type.

Also interesting information we can get from the dynamic of the metric breaked by onboarding types - we see that at the first days of April the new type Voice ratio is much better than the old type ratio, but after a few weeks it becomes worse than the old one. That can be the issue for further investigation. 
