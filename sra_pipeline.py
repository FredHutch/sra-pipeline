#!/usr/bin/env python3

"""
Utility for working with SRA pipeline jobs.
"""

import argparse
import datetime
import io
import json
import os
import re
import sys
from time import sleep

from multiprocessing.pool import ThreadPool
from collections import defaultdict
from math import ceil
from urllib.parse import urlparse

import boto3
from botocore.exceptions import ClientError
import numpy as np
import sh

# import pandas as pd

PREFIX = "pipeline-results"
CSV_FILE = "salivary_sizes.csv"

RETRY_EXCEPTIONS = ("ProvisionedThroughputExceededException", "ThrottlingException")


def get_git_branch():
    "get the current git branch"
    headfile = os.path.join(get_script_directory(), ".git", "HEAD")
    with open(headfile) as filehandle:
        lines = filehandle.readlines()
    headline = [x.strip() for x in lines if x.startswith("ref:")]
    if not headline:
        return None
    return headline[0].replace("ref: refs/heads/", "")


def get_script_directory():
    "get full path to directory the running script is in"
    pathname = os.path.dirname(sys.argv[0])
    return os.path.abspath(pathname)


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


def get_env_var(job, env_var):
    "get the value of a specified environment variable from a job description"
    hsh = {}
    for item in job["container"]["environment"]:
        hsh[item["name"]] = item["value"]
    return hsh[env_var]


def show_completed(job_id):
    "show completed accession numbers"
    s3 = boto3.client("s3")  # pylint: disable=invalid-name
    batch = boto3.client("batch")
    resp = batch.describe_jobs(jobs=[job_id])["jobs"]
    if not resp:
        print("No information on this job.")
        sys.exit(1)
    job = resp[0]
    num_viruses = int(job["jobName"].split("-")[-1])

    completed_map = defaultdict(list)
    args = dict(
        Bucket=get_env_var(job, "BUCKET_NAME"),
        Prefix=get_env_var(job, "PREFIX"),
        MaxKeys=999,
    )
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
        x for x in completed_map.keys() if len(completed_map[x]) == num_viruses
    ]
    return completed


def show_in_progress(job_id):  # pylint: disable=too-many-locals
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

    completed = set(show_completed(job_id))
    ret = set(accession_nums) - completed
    return list(ret)


def show_remaining(job_id, completed):
    "show items still remaining in this job"
    batch = boto3.client("batch")
    job = batch.describe_jobs(jobs=[job_id])["jobs"][0]

    accession_list = get_env_var(job, "ACCESSION_LIST")
    parsed_url = urlparse(accession_list)
    bucket = parsed_url.netloc
    path = parsed_url.path.lstrip("/")
    s3 = boto3.client("s3")  # pylint: disable=invalid-name
    obj = s3.get_object(Bucket=bucket, Key=path)
    accstr = obj["Body"].read().decode("utf-8")
    all_sras = accstr.split("\n")
    return set(all_sras) - set(completed)


# def select_from_csv(num_rows, method):
#     """
#     Selects accession numbers from the csv file.
#     Args:
#         num_rows (int): the number of accession numbers to return.
#                         Will return all available rows if this number
#                         is larger than the number of rows.
#         method (str): one of "random" or "small". "random" selects accession
#                       numbers randomly; "small"  selects them by size
#                       (in ascending order).
#     """
#     if not method in ["small", "random"]:
#         raise ValueError("invalid method! must be 'small' or 'random'")
#     raw_df = pd.read_csv(CSV_FILE)
#     exclude = []
#     exclude.extend(show_completed())
#     exclude.extend(show_in_progress())
#     df0 = raw_df[
#         ~raw_df["accession_number"].isin(exclude)
#     ]  # pylint: disable=invalid-name
#     nrow = df0.shape[0]
#     if num_rows > nrow:
#         num_rows = nrow
#     if num_rows < 1:
#         print("no SRAs left to process.")
#         sys.exit(1)
#     if method == "small":
#         return df0["accession_number"].head(num_rows).tolist()
#     return df0["accession_number"].sample(num_rows).tolist()


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
    references, filename=None, prefix=None, delete_file=False
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
    # else:
    #     accession_nums = select_from_csv(num_rows, method)

    bytesarr = bytearray("\n".join(accession_nums), "utf-8")
    bytesio = io.BytesIO(bytesarr)
    # subtract 1 because of header line:
    job_size = len(accession_nums) - 1
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
    raw_env = dict(
        BATCH_FILE_TYPE="script",
        BATCH_FILE_URL="https://raw.githubusercontent.com/FredHutch/sra-pipeline/{}/run.py".format(
            get_git_branch()
        ),
        BUCKET_NAME="fh-pi-jerome-k",
        PREFIX=prefix,
        ACCESSION_LIST=url,
        NUM_CORES=cpus,
        REFERENCES=references,
    )
    if os.getenv("DISABLE_SLEEP"):
        raw_env["DISABLE_SLEEP"] = "True"
    env = to_aws_env(raw_env)
    args = dict(
        jobName=job_name,
        jobQueue="mixed",
        jobDefinition=jobdef,
        containerOverrides=dict(environment=env),
    )
    if job_size > 1:
        args["arrayProperties"] = dict(size=job_size)

    # uncomment this:
    res = batch.submit_job(**args)

    if delete_file:
        if os.path.exists(filename):
            os.remove(filename)

    del res["ResponseMetadata"]
    return res


# def submit_small(num_jobs, references):
#     "submit <num_jobs> jobs of ascending size"
#     return submit(num_jobs, "small", references)


# def submit_random(num_jobs, references):
#     "submit <num_jobs> randomly chosen jobs"
#     return submit(num_jobs, "random", references)


def submit_file(filename, references, prefix=None):
    "submit accession numbers from filename"
    return submit(references, filename, prefix)

    # references, filename=None, prefix=None, delete_file=False


def submit_synapse(synapse_id, references, prefix):
    """
    create a job to process all fastq files 'under'
    the given synapse id
    """

    synapse_tsv_file = "{}.tsv".format(synapse_id)
    with open(synapse_tsv_file, "w") as synapse_fh:
        synapse_fh.write("file.id\tfile.name\n")
        for line in sh.synapse("list", "-r", synapse_id, _iter=True):
            line = line.strip()
            if not line.endswith(".bam"):
                continue
            line = re.sub(' +', '\t', line)
            synapse_fh.write(line)
            synapse_fh.write("\n")

    # remove this after testing
    sh.head("-2", synapse_tsv_file, _out="tmp.tsv")
    os.remove(synapse_tsv_file)
    os.rename("tmp.tsv", synapse_tsv_file)
    # end section to remove

    return submit(
        references, filename=synapse_tsv_file, prefix=prefix, delete_file=True
    )


def main():
    "do the work"
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument(
        "-c",
        "--completed",
        help="show completed accession numbers",
        type=str,
        metavar="JOB_ID",
    )
    parser.add_argument(
        "-i",
        "--in-progress",
        help="show accession numbers that are in progress",
        type=str,
        metavar="JOB_ID",
    )
    # parser.add_argument(
    #     "-s",
    #     "--submit-small",
    #     help="submit N jobs of ascending size",
    #     type=int,
    #     metavar="N",
    # )
    # parser.add_argument(
    #     "-r",
    #     "--submit-random",
    #     help="submit N randomly chosen jobs",
    #     type=int,
    #     metavar="N",
    # )
    parser.add_argument(
        "-r",
        "--remaining",
        help="show remaining items (not yet completed)",
        type=str,
        metavar="JOB_ID",
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
    parser.add_argument(
        "-s",
        "--synapse-id",
        help="run against all bam files listed under SYNAPSE_ID",
        type=str,
        metavar="SYNAPSE_ID",
    )

    args = parser.parse_args()

    if args.submit_file:
        if not args.references:
            print(
                "You must supply a comma-separated list of references with the -y flag."
            )
            sys.exit(1)

    if len(sys.argv) == 1:
        print("invoke with --help to see usage information.")
        sys.exit(1)
    if args.completed:
        completed = show_completed(args.completed)
        for item in completed:
            print(item)
    elif args.remaining:
        completed = show_completed(args.remaining)
        remaining = show_remaining(args.remaining, completed)
        for item in remaining:
            print(item)

    elif args.in_progress:
        in_progress = show_in_progress(args.in_progress)
        for item in in_progress:
            print(item)
    # elif args.submit_small:
    #     result = submit_small(args.submit_small, args.references)
    #     print(json.dumps(result, sort_keys=True, indent=4))
    # elif args.submit_random:
    #     result = submit_random(args.submit_random, args.references)
    #     print(json.dumps(result, sort_keys=True, indent=4))
    elif args.submit_file:
        result = submit_file(args.submit_file, args.references, args.prefix)
        print(json.dumps(result, sort_keys=True, indent=4))
    elif args.job_id:
        result = search_logs(args.job_id, args.query)
        for item in result:
            print(item)
    elif args.synapse_id:
        result = submit_synapse(args.synapse_id, args.references, args.prefix)
        print(json.dumps(result, sort_keys=True, indent=4))


if __name__ == "__main__":
    main()
