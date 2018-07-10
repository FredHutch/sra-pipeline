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
from time import sleep

from multiprocessing.pool import ThreadPool
from collections import defaultdict
from math import ceil
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
import numpy as np
import pandas as pd

# TODO change this if we have a different number of viruses
# NUM_VIRUSES=3
NUM_VIRUSES = 4
PREFIX = "pipeline-results"
CSV_FILE = "salivary_sizes.csv"

RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")


def inspect_logs(args):  # index, batch, logs, job_id, search_string):
    "parallelizable(?) function to look at logs for a single child"
    index = args["index"]
    search_string = args["search_string"]
    job_id = args["job_id"]
    batch = boto3.client("batch")
    logs = boto3.client("logs")
    child_id = "{}:{}".format(job_id, index)
    child_desc = batch.describe_jobs(jobs=[child_id])["jobs"][0]
    if not "container" in child_desc:
        return False
    if not "logStreamName" in child_desc["container"]:
        return False
    lsn = child_desc["container"]["logStreamName"]
    args = dict(logGroupName="/aws/batch/job", logStreamName=lsn)
    retries = 0
    while True:
        try:
            resp = logs.get_log_events(**args)
            if not resp["events"]:
                return False
            if "nextBackwardToken" in resp:
                args["nextToken"] = resp["nextBackwardToken"]
            for event in resp["events"]:
                if search_string in event["message"]:
                    return True
        except ClientError as err:
            if err.response["Error"]["Code"] not in RETRY_EXCEPTIONS:
                raise
            # print("retrying...")
            sleep(2 ** retries)
            retries += 1  # TODO max retries


def search_logs(job_id, search_string):
    "search logs for a given string, return child indices where found"
    batch = boto3.client("batch")
    resp = batch.describe_jobs(jobs=[job_id])
    if not "jobs" in resp:
        raise ValueError("no such job")
    job = resp["jobs"][0]
    if not "arrayProperties" in job:
        raise ValueError("this is not an array job")
    size = job["arrayProperties"]["size"]
    iargs = []
    for index in range(size):
        iargs.append(dict(job_id=job_id, search_string=search_string, index=index))

    pool_size = 12
    with ThreadPool(pool_size) as pool:
        results = pool.map(inspect_logs, iargs)

    return [i for i, x in enumerate(results) if x]


def get_failsons(batch, job_id):
    """
    get ids of children that have failed
    """
    args = dict(arrayJobId=job_id, jobStatus="FAILED")
    failsons = []
    while True:
        response = batch.list_jobs(**args)
        if not "jobSummaryList" in response or not response["jobSummaryList"]:
            return []

        jsl = response["jobSummaryList"]
        failsons.extend([x["arrayProperties"]["index"] for x in jsl])
        try:
            args["nextToken"] = response["nextToken"]
        except KeyError:
            break
    return set(failsons)


def show_completed():
    "show completed accession numbers"
    s3 = boto3.client("s3")  # pylint: disable=invalid-name
    completed_map = defaultdict(list)
    args = dict(Bucket="fh-pi-jerome-k", Prefix=PREFIX, MaxKeys=999)
    while True:
        response = s3.list_objects_v2(**args)
        if not "Contents" in response:
            return []
        for item in response["Contents"]:
            segs = item["Key"].split("/")
            accession = segs[1]
            virus = segs[2]
            completed_map[accession].append(virus)
        try:
            args["ContinuationToken"] = response["NextContinuationToken"]
        except KeyError:
            break
    completed = [
        x for x in completed_map.keys() if len(completed_map[x]) == NUM_VIRUSES
    ]
    return completed


def show_in_progress():  # pylint: disable=too-many-locals
    "show accession numbers that are in progress"
    s3 = boto3.client("s3")  # pylint: disable=invalid-name
    batch = boto3.client("batch")
    in_progress_states = ["SUBMITTED", "PENDING", "RUNNABLE", "STARTING", "RUNNING"]
    state_jobs = []
    for state in in_progress_states:
        results = batch.list_jobs(jobQueue="mixed", jobStatus=state)
        state_jobs.extend(results["jobSummaryList"])
    job_ids = [x["jobId"] for x in state_jobs]
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
        jobs.extend(response["jobs"])
    accession_lists_map = {}
    for job in jobs:
        if "container" in job and "environment" in job["container"]:
            for item in job["container"]["environment"]:
                if item["name"] == "ACCESSION_LIST":
                    accession_lists_map[item["value"]] = get_failsons(
                        batch, job["jobId"]
                    )
    accession_nums = []
    for item, failsons in accession_lists_map.items():
        url = urlparse(item)
        bucket = url.netloc
        key = url.path.lstrip("/")
        flh = io.BytesIO()
        s3.download_fileobj(bucket, key, flh)
        tmp = flh.getvalue().decode("utf-8").strip().split("\n")
        tmp = [x for i, x in enumerate(tmp) if not i in failsons]
        accession_nums.extend(tmp)

    completed = set(show_completed())
    ret = set(accession_nums) - completed
    return list(ret)


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
    if not method in ["small", "random"]:
        raise ValueError("invalid method! must be 'small' or 'random'")
    raw_df = pd.read_csv(CSV_FILE)
    exclude = []
    exclude.extend(show_completed())
    exclude.extend(show_in_progress())
    df0 = raw_df[
        ~raw_df["accession_number"].isin(exclude)
    ]  # pylint: disable=invalid-name
    nrow = df0.shape[0]
    if num_rows > nrow:
        num_rows = nrow
    if num_rows < 1:
        print("no SRAs left to process.")
        sys.exit(1)
    if method == "small":
        return df0["accession_number"].head(num_rows).tolist()
    return df0["accession_number"].sample(num_rows).tolist()


def to_aws_env(env):
    "convert dict to name/value pairs"
    out = []
    for key, val in env.items():
        out.append(dict(name=key, value=val))
    return out


def get_latest_jobdef_revision(batch_client, jobdef_name):  # FIXME handle pagination
    "get the most recent revision for a job definition"
    results = batch_client.describe_job_definitions(
        status="ACTIVE", jobDefinitionName=jobdef_name
    )["jobDefinitions"]
    if not results:
        raise ValueError("No job definition called {}.".format(jobdef_name))
    jobdef = max(results, key=lambda x: x["revision"])  # ['revision']
    revision = jobdef["revision"]
    cpus = str(jobdef["containerProperties"]["vcpus"])
    return (revision, cpus)


def submit(
    num_rows, method, references, filename=None, prefix=None
):  # pylint: disable=too-many-locals
    """
    Utility function to submit jobs.
    Args:
        num_rows: Number of accession numbers to process, passed to select_from_csv()
        method: 'random', 'small' (passed to select_from_csv()), or 'filename'
        filename: list of accession numbers
        prefix: optional s3 prefix at which to write output
    """
    s3 = boto3.client("s3")  # pylint: disable=invalid-name
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
    s3.upload_fileobj(
        bytesio, "fh-pi-jerome-k", "sra-submission-manifests/{}".format(key)
    )
    reflen = len(references.split(","))
    job_name = "sra-pipeline-{}-{}-{}-refs-{}".format(
        os.getenv("USER"), nowstr, job_size, reflen
    )
    job_def_name = (
        "sra-pipeline"
    )  # use "hello" for testing, "sra-pipeline" for production
    revision, cpus = get_latest_jobdef_revision(batch, job_def_name)
    jobdef = "{}:{}".format(job_def_name, revision)
    if not prefix:
        prefix = PREFIX
    env = to_aws_env(
        dict(
            BUCKET_NAME="fh-pi-jerome-k",
            PREFIX=prefix,
            ACCESSION_LIST=url,
            NUM_CORES=cpus,
            REFERENCES=references,
        )
    )
    args = dict(
        jobName=job_name,
        jobQueue="mixed",
        jobDefinition=jobdef,
        containerOverrides=dict(environment=env),
    )
    if job_size > 1:
        args["arrayProperties"] = dict(size=job_size)
    res = batch.submit_job(**args)

    del res["ResponseMetadata"]
    return res


def submit_small(num_jobs, references):
    "submit <num_jobs> jobs of ascending size"
    return submit(num_jobs, "small", references)


def submit_random(num_jobs, references):
    "submit <num_jobs> randomly chosen jobs"
    return submit(num_jobs, "random", references)


def submit_file(filename, references, prefix=None):
    "submit accession numbers from filename"
    return submit(0, "file", references, filename, prefix)


def main():
    "do the work"
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-c",
        "--completed",
        help="show completed accession numbers",
        action="store_true",
    )
    parser.add_argument(
        "-i",
        "--in-progress",
        help="show accession numbers that are in progress",
        action="store_true",
    )
    parser.add_argument(
        "-s",
        "--submit-small",
        help="submit N jobs of ascending size",
        type=int,
        metavar="N",
    )
    parser.add_argument(
        "-r",
        "--submit-random",
        help="submit N randomly chosen jobs",
        type=int,
        metavar="N",
    )
    parser.add_argument(
        "-f",
        "--submit-file",
        help="submit accession numbers contained in FILE",
        type=str,
        metavar="FILE",
    )
    parser.add_argument(
        "-p",
        "--prefix",
        help="override default prefix",
        default=PREFIX,
        type=str,
        metavar="PREFIX",
    )
    parser.add_argument(
        "-q",
        "--query",
        help="string to search for in logs, must specify JOB_ID",
        type=str,
        metavar="STR",
        default="finished downloading",
    )
    parser.add_argument(
        "job_id", nargs="?", help="a job ID to search the logs of (use with -q only)"
    )
    parser.add_argument(
        "-y",
        "--references",
        help="comma-separated list of references",
        type=str,
        metavar="REFERENCES",
    )

    args = parser.parse_args()

    if args.submit_small or args.submit_random or args.submit_file:
        if not args.references:
            print(
                "You must supply a comma-separated list of references with the -y flag."
            )
            sys.exit(1)

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
        result = submit_small(args.submit_small, args.references)
        print(json.dumps(result, sort_keys=True, indent=4))
    elif args.submit_random:
        result = submit_random(args.submit_random, args.references)
        print(json.dumps(result, sort_keys=True, indent=4))
    elif args.submit_file:
        result = submit_file(args.submit_file, args.prefix, args.references)
        print(json.dumps(result, sort_keys=True, indent=4))
    elif args.job_id:
        result = search_logs(args.job_id, args.query)
        for item in result:
            print(item)


if __name__ == "__main__":
    main()
