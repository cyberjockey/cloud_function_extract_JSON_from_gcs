"""Import a json file into BigQuery."""

import logging
import os
import re
import json
import requests
from io import StringIO
from string import digits
from google.cloud import bigquery

GCP_PROJECT = os.environ.get('GCP_PROJECT')


def bigqueryImport(data, context):
    """Import a json file into BigQuery."""
    # get storage update data
    bucketname = data['bucket']
    filename = data['name']
    timeCreated = data['timeCreated']

    # check filename format - dataset_name/table_name.json
    #if not re.search('^[a-zA-Z_-]+.[a-zA-Z_-]+.json$', filename):
    #    logging.error('Unrecognized filename format: %s' % (filename))
    #    return
    
    # need converter json to ndjson?
    # file_name_converted = [json.dumps(record) for record in json.loads(filename)]
    

    # parse filename
    remove_digits = str.maketrans('', '', digits)
    datasetname, tablename = filename.replace('eustacio_','').replace('horus_','').replace('isis_','').replace('maat_','').replace('nun_','').replace('prunto_','').replace('sparado_','').replace('sakkara_','').replace('tajet_','').replace('thoth_','').replace('valuto_','').replace('.json','').replace('-','').translate(remove_digits).split('.public.')
    table_id = '%s.%s.%s' % (GCP_PROJECT, datasetname, tablename)

    # log the receipt of the file
    uri = 'gs://%s/%s' % (bucketname, filename)
    print('Received file "%s" at %s.' % (
        uri,
        timeCreated
    ))

    # create bigquery client
    client = bigquery.Client()

    # get dataset reference
    dataset_ref = client.dataset(datasetname)

    # check if dataset exists, otherwise create
    try:
        client.get_dataset(dataset_ref)
    except Exception:
        logging.warn('Creating dataset: %s' % (datasetname))
        client.create_dataset(dataset_ref)

    # create a bigquery load job config
    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True
    job_config.create_disposition = 'CREATE_IF_NEEDED',
    job_config.source_format = 'NEWLINE_DELIMITED_JSON',
    job_config.write_disposition = 'WRITE_TRUNCATE',

    # create a bigquery load job
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
        
    #send data to n8n
    
    webhookurl = 'https://stupid-moose-83.hooks.n8n.cloud/webhook-test/53063239-7382-4677-b7da-2ba292f873d3'
    data = { 'datasets' : dataset_ref }
    r= requests.post(webhookurl, data=json.dumps(data), headers={'Content-Type': 'application/json'})
    return r
