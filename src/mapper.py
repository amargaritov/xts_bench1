import boto3
import json
import random
import resource
from io import StringIO ## for Python 3
import time

# create an S3 session
s3 = boto3.resource('s3')
s3_client = boto3.client('s3')

# constants
INPUT_MAPPER_PREFIX = "artemiy/input/"
OUTPUT_MAPPER_PREFIX = "artemiy/task/mapper/";
INPUT_REDUCER_PREFEFIX = OUTPUT_MAPPER_PREFIX
OUTPUT_REDUCER_PREFIX = "artemiy/task/reducer/";

def write_to_s3(bucket, key, data, metadata):
    s3.Bucket(bucket).put_object(Key=key, Body=data, Metadata=metadata)

def mapper(event):
    
    start_time = time.time()

    dest_bucket = event['destBucket']  # s3 bucket where the mapper will write the result
    src_bucket  = event['srcBucket']   # s3 bucket where the mapper will search for input files
    src_keys    = event['keys']        # src_keys is a list of input file names for this mapper
    job_id      = event['jobId']
    mapper_id   = event['mapperId']
   
    # aggr 
    output = {}
    line_count = 0
    err = ''

    # INPUT CSV => OUTPUT JSON

    for key in src_keys:
        key = INPUT_MAPPER_PREFIX + key
        response = s3_client.get_object(Bucket=src_bucket, Key=key)
        contents = response['Body'].read().decode("utf-8") 
    
        for line in contents.split('\n')[:-1]:
            line_count +=1
            try:
                data = line.split(',')
                srcIp = data[0][:8]
                if srcIp not in output:
                    output[srcIp] = 0
                output[srcIp] += float(data[3])
            except getopt.GetoptError as e:
#                print (e)
                err += '%s' % e

    time_in_secs = (time.time() - start_time)
    #timeTaken = time_in_secs * 1000000000 # in 10^9 
    #s3DownloadTime = 0
    #totalProcessingTime = 0 
    pret = [len(src_keys), line_count, time_in_secs, err]
    mapper_fname = "%sjob_%s/map_%s" % (OUTPUT_MAPPER_PREFIX, job_id, mapper_id) 
    print(mapper_fname)
    metadata = {
                    "linecount":  '%s' % line_count,
                    "processingtime": '%s' % time_in_secs,
                    "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
               }
    print ("metadata", metadata)
    write_to_s3(dest_bucket, mapper_fname, json.dumps(output), metadata)
    return pret

ev = {
   "srcBucket": "storage-module-test", 
   "destBucket": "storage-module-test", 
   "keys": ["part-00000"],
   "jobId": "0",
   "mapperId": 0,
     }
mapper(ev)

def reducer(event):
    
    start_time = time.time()
    
    dest_bucket = event['destBucket']  # s3 bucket where the mapper will write the result
    src_bucket  = event['srcBucket']   # s3 bucket where the mapper will search for input files
    reducer_keys = event['keys']    # reducer_keys is a list of input file names for this mapper
    job_id = event['jobId']
    r_id = event['reducerId']
    n_reducers = event['nReducers']
    
    # aggr 
    results = {}
    line_count = 0

    # INPUT JSON => OUTPUT JSON

    # Download and process all keys
    for key in reducer_keys:
        key = INPUT_REDUCER_PREFIX + key
        response = s3_client.get_object(Bucket=job_bucket, Key=key)
        contents = response['Body'].read().decode("utf-8")

        try:
            for srcIp, val in json.loads(contents).iteritems():
                line_count +=1
                if srcIp not in results:
                    results[srcIp] = 0
                results[srcIp] += float(val)
        except getopt.GetoptError as e:
            print (e)

    time_in_secs = (time.time() - start_time)
    #timeTaken = time_in_secs * 1000000000 # in 10^9 
    #s3DownloadTime = 0
    #totalProcessingTime = 0 
    pret = [len(reducer_keys), line_count, time_in_secs]
    print ("Reducer ouputput", pret)

    if n_reducers == 1:
        # Last reducer file, final result
        fname = "%sjob_%s/result" % (OUTPUT_REDUCER_PREFIX, job_id)
    else:
        fname = "%sjob_%s/reducer_%s" % (OUTPUT_REDUCER_PREFIX, job_id, r_id)
    
    metadata = {
                    "linecount":  '%s' % line_count,
                    "processingtime": '%s' % time_in_secs,
                    "memoryUsage": '%s' % resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
               }

    write_to_s3(job_bucket, fname, json.dumps(results), metadata)
    return pret

'''
ev = {
    "srcBucket": "storage-module-test", 
    "destBucket": "storage-module-test",
    "keys": ["0"],
    "nReducers": 1,
    "jobId": "0",
    "reducerId": 0, 
}
reducer(ev)
'''
