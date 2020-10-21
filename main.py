
import logging
import os
import re

from google.cloud import bigquery

GCP_PROJECT = os.environ.get('GCP_PROJECT')


def bigqueryImport(data, context):

    bucketname = data['bucket']
    filename = data['name']
    timeCreated = data['timeCreated']

    if not re.search('^[a-zA-Z_-]+1[a-zA-Z_-]+.json$', filename):
        logging.error('Unrecognized filename format: %s' % (filename))
        return

    
    datasetname, tablename = filename.replace('.json', '').split('1')
    table_id = '%s.%s.%s' % (GCP_PROJECT, datasetname, tablename)

    uri = 'gs://%s/%s' % (bucketname, filename)
    print('Received file "%s" at %s.' % (
        uri,
        timeCreated
    ))

    client = bigquery.Client()

    dataset_ref = client.dataset(datasetname)

    try:
        client.get_dataset(dataset_ref)
    except Exception:
        logging.warn('Creating dataset: %s' % (datasetname))
        client.create_dataset(dataset_ref)

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.create_disposition = 'CREATE_IF_NEEDED',
    job_config.source_format = 'NEWLINE_DELIMITED_JSON',
    job_config.write_disposition = 'WRITE_TRUNCATE',

    try:
        load_job = client.load_table_from_uri(
            uri,
            table_id,
            job_config=job_config,
        )
        print('Load job: %s [%s]' % (
            load_job.job_id,
            table_id
        ))
    except Exception as e:
        logging.error('Failed to create load job: %s' % (e))
