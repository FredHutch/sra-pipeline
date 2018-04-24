#!/usr/bin/env python3

"""
Utility for working with SRA pipeline jobs.
"""

import argparse
import datetime
import io
import json
import os
import sys

from collections import defaultdict
from math import ceil
from urllib.parse import urlparse

import boto3
import numpy as np
import pandas as pd


def done_downloading(job_id):
    "show which children are done downloading"
    logs = boto3.client("logs")
    batch = boto3.client("batch")
    resp = batch.describe_jobs(jobs=[job_id])
    if not 'jobs' in resp:
        raise ValueError("no such job")
    job = resp['jobs'][0]
    if not 'arrayProperties' in job:
        raise ValueError("this is not an array job")
    size = job['arrayProperties']['size']
    out = {}
    for index in range(size):
        child_id = "{}:{}".format(job_id, index)
        child_desc = batch.describe_jobs(jobs=[child_id])['jobs'][0]
        if not 'container' in child_desc:
            raise ValueError("'container' key not found")
        if not 'logStreamName' in child_desc['container']:
            raise ValueError("'logStreamName' not found")
        lsn = child_desc['container']['logStreamName']
        args = dict(logGroupName="/aws/batch/job", logStreamName=lsn)
        while True:
            resp = logs.get_log_events(**args)
            if not resp['events']:
                break
            if 'nextBackwardToken' in resp:
                args['nextToken'] = resp['nextBackwardToken']
            for event in resp['events']:
                if "finished downloading" in event['message']:
                    out[index] = 1
                    break
    return sorted(list(out.keys()))




def show_completed():
    "show completed accession numbers"
    s3 = boto3.client("s3") # pylint: disable=invalid-name
    completed_map = defaultdict(list)
    args = dict(Bucket="fh-pi-jerome-k", Prefix="pipeline-results2", MaxKeys=999)
    while True:
        response = s3.list_objects_v2(**args)
        if not 'Contents' in response:
            return []
        for item in response['Contents']:
            segs = item['Key'].split("/")
            accession = segs[1]
            virus = segs[2]
            completed_map[accession].append(virus)
        try:
            args['ContinuationToken'] = response['NextContinuationToken']
        except KeyError:
            break
    completed = [x for x in completed_map.keys() if len(completed_map[x]) == 3]
    return completed


def show_in_progress(): # pylint: disable=too-many-locals
    "show accession numbers that are in progress"
    s3 = boto3.client("s3") # pylint: disable=invalid-name
    batch = boto3.client("batch")
    in_progress_states = ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']
    state_jobs = []
    for state in in_progress_states:
        results = batch.list_jobs(jobQueue="mixed", jobStatus=state)
        state_jobs.extend(results['jobSummaryList'])
    job_ids = [x['jobId'] for x in state_jobs]
    if not job_ids:
        return []
    chunks = []
    jobs = []
    if len(job_ids) <= 100:
        chunks.append(job_ids)
    else:
        num_chunks = int(ceil(len(job_ids) / 100.0))
        chunks = np.array_split(job_ids, num_chunks)
    for chunk in chunks:
        response = batch.describe_jobs(jobs=chunk)
        jobs.extend(response['jobs'])
    accession_lists_map = {}
    for job in jobs:
        if 'container' in job and 'environment' in job['container']:
            for item in job['container']['environment']:
                if item['name'] == 'ACCESSION_LIST':
                    accession_lists_map[item['value']] = 1
    accession_nums = []
    for item in accession_lists_map:
        url = urlparse(item)
        bucket = url.netloc
        key = url.path.lstrip("/")
        flh = io.BytesIO()
        s3.download_fileobj(bucket, key, flh)
        accession_nums.extend(flh.getvalue().decode('utf-8').strip().split("\n"))
    return accession_nums



def select_from_csv(num_rows, method):
    """
    Selects accession numbers from the csv file.
    Args:
        num_rows (int): the number of accession numbers to return.
                        Will return all available rows if this number
                        is larger than the number of rows.
        method (str): one of "random" or "small". "random" selects accession
                      numbers randomly; "small"  selects them by size
                      (in ascending order).
    """
    if not method in ['small', 'random']:
        raise ValueError("invalid method! must be 'small' or 'random'")
    raw_df = pd.read_csv("srr-sizes.csv")
    exclude = []
    exclude.extend(show_completed())
    exclude.extend(show_in_progress())
    df = raw_df[~raw_df['accession_number'].isin(exclude)] # pylint: disable=invalid-name
    nrow = df.shape[0]
    if num_rows > nrow:
        num_rows = nrow
    if method == "small":
        return df['accession_number'].head(num_rows).tolist()
    return df['accession_number'].sample(num_rows).tolist()

def to_aws_env(env):
    "convert dict to name/value pairs"
    out = []
    for key, val in env.items():
        out.append(dict(name=key, value=val))
    return out


def get_latest_jobdef_revision(batch_client, jobdef_name): # FIXME handle pagination
    "get the most recent revision for a job definition"
    results = \
      batch_client.describe_job_definitions(status="ACTIVE",
                                            jobDefinitionName=jobdef_name)['jobDefinitions']
    if not results:
        raise ValueError("No job definition called {}.".format(jobdef_name))
    return max(results, key=lambda x: x['revision'])['revision']

def submit(num_rows, method, filename=None): # pylint: disable=too-many-locals
    """
    Utility function to submit jobs.
    Args:
        num_rows: Number of accession numbers to process, passed to select_from_csv()
        method: 'random', 'small' (passed to select_from_csv()), or 'filename'
        filename: list of accession numbers
    """
    s3 = boto3.client("s3") # pylint: disable=invalid-name
    batch = boto3.client("batch")
    now = datetime.datetime.now()
    nowstr = now.strftime("%Y%m%d%H%M%S")
    if filename:
        with open(filename, "r") as fileh:
            accession_nums = fileh.readlines()
            accession_nums = [x.strip() for x in accession_nums]
    else:
        accession_nums = select_from_csv(num_rows, method)
    bytesarr = bytearray("\n".join(accession_nums), "utf-8")
    bytesio = io.BytesIO(bytesarr)
    job_size = len(accession_nums)
    key = "{}-{}.txt".format(nowstr, job_size)
    url = "s3://fh-pi-jerome-k/sra-submission-manifests/{}".format(key)
    s3.upload_fileobj(bytesio, "fh-pi-jerome-k", "sra-submission-manifests/{}".format(key))
    job_name = "sra-pipeline-{}-{}-{}".format(os.getenv("USER"), nowstr, job_size)
    env = to_aws_env(dict(BUCKET_NAME="fh-pi-jerome-k", PREFIX="pipeline-results2",
                          ACCESSION_LIST=url))
    job_def_name = "sra-pipeline" # use "hello" for testing, "sra-pipeline" for production
    jobdef = "{}:{}".format(job_def_name, get_latest_jobdef_revision(batch, job_def_name))
    res = batch.submit_job(jobName=job_name, jobQueue="mixed",
                           arrayProperties=dict(size=job_size),
                           jobDefinition=jobdef,
                           containerOverrides=dict(environment=env))

    del res['ResponseMetadata']
    return res

def submit_small(num_jobs):
    "submit <num_jobs> jobs of ascending size"
    return submit(num_jobs, "small")

def submit_random(num_jobs):
    "submit <num_jobs> randomly chosen jobs"
    return submit(num_jobs, "random")

def submit_file(filename):
    "submit accession numbers from filename"
    return submit(0, "file", filename)

def main():
    "do the work"
    parser = argparse.ArgumentParser()
    parser.add_argument("-c", "--completed", help="show completed accession numbers",
                        action="store_true")
    parser.add_argument("-i", "--in-progress",
                        help="show accession numbers that are in progress",
                        action="store_true")
    parser.add_argument("-s", "--submit-small", help="submit N jobs of ascending size",
                        type=int, metavar='N')
    parser.add_argument("-r", "--submit-random", help="submit N randomly chosen jobs",
                        type=int, metavar='N')
    parser.add_argument("-f", "--submit-file", help="submit accession numbers contained in FILE",
                        type=str, metavar='FILE')
    parser.add_argument("-d", "--done-downloading",
                        help="show which children in JOB_ID are done downloading",
                        type=str, metavar="JOB_ID")

    args = parser.parse_args()
    if len(sys.argv) == 1:
        print("invoke with --help to see usage information.")
        sys.exit(1)
    if args.completed:
        completed = show_completed()
        for item in completed:
            print(item)
    elif args.in_progress:
        in_progress = show_in_progress()
        for item in in_progress:
            print(item)
    elif args.submit_small:
        result = submit_small(args.submit_small)
        print(json.dumps(result, sort_keys=True, indent=4))
    elif args.submit_random:
        result = submit_random(args.submit_random)
        print(json.dumps(result, sort_keys=True, indent=4))
    elif args.submit_file:
        result = submit_file(args.submit_file)
        print(json.dumps(result, sort_keys=True, indent=4))
    elif args.done_downloading:
        result = done_downloading(args.done_downloading)
        for item in result:
            print(item)

if __name__ == "__main__":
    main()
