#!/usr/bin/env python3

"script to run on AWS batch instance"

import contextlib
import datetime
import glob
import os
import os.path
from pathlib import Path
import random
import sys
import time

import sh
import requests

HOME = os.getenv("HOME")
PTMP = "tmp"


@contextlib.contextmanager
def working_directory(path):
    """Changes working directory and returns to previous on exit."""
    prev_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(prev_cwd)


def get_metadata():
    "get ec2 metadata if available"
    try:
        return requests.get(
            "http://169.254.169.254/latest/meta-data/public-hostname", timeout=1
        )
    except requests.exceptions.Timeout:
        return "unknown"


def get_container_id():
    "get container id"
    # container_id=$(cat /proc/self/cgroup | head -n 1 | cut -d '/' -f4)
    id_ = sh.cut(
        sh.head(sh.cat("/proc/self/cgroup"), "-n", "1"), "-d", "/", "-f4"
    ).strip()
    if not id_:
        return "unknown"
    return id_


def configure_aws():
    "configure aws"
    params = {
        "default.s3.multipart_chunksize": "50MB",
        "default.s3.max_concurrent_requests": "100",
        "default.s3.max_queue_size": "10000",
        "default.s3.multipart_threshold": "64MB",
    }
    for key, value in params.items():
        sh.aws("configure", "set", key, value)


def ensure_correct_environment():
    "ensure correct environment"
    if not os.getenv("NUM_CORES"):
        print("NUM_CORES is not set, exiting")
        sys.exit(1)


def setup_scratch():
    "sets up scratch, returns scratch dir and sra accession number"
    sh.aws("s3", "cp", os.getenv("ACCESSION_LIST"), "accessionlist.txt")
    if os.getenv("AWS_BATCH_JOB_ID"):
        print("this is a batch job")
        sh.rm("-rf", "{}/ncbi".format(HOME))
        if os.getenv("AWS_BATCH_JOB_ARRAY_INDEX"):
            print("this is an array job")
            line = int(os.getenv("AWS_BATCH_JOB_ARRAY_INDEX")) + 1
            sra_accession = sh.sed("{}q;d".format(line), "accessionlist.txt").strip()
            scratch = "/scratch/{}/{}/".format(
                os.getenv("AWS_BATCH_JOB_ID"), os.getenv("AWS_BATCH_JOB_ARRAY_INDEX")
            )
        else:
            print("this is not an array job")
            sra_accession = sh.sed("1q;d", "accessionlist.txt").strip()
            scratch = "/scratch/{}/".format(os.getenv("AWS_BATCH_JOB_ID"))
        sh.mkdir("-p", scratch)
        sh.ln("-s", scratch, "{}/ncbi".format(HOME))
        sh.mkdir("-p", "{}/ncbi/dbGaP-17102".format(HOME))
    else:
        print("this is not an aws batch job")
        sra_accession = sh.sed("1q;d", "accessionlist.txt").strip()
        scratch = "."
        sh.mkdir("-p", "{}/ncbi/dbGaP-17102".format(HOME))
    return scratch, sra_accession


def get_fastq_files_from_s3(sra_accession):
    """
    If fastq files are present in S3, download them and return True.
    Otherwise return False.
    """
    bucket = os.getenv("BUCKET_NAME")
    dirs = ["pipeline-fastq", "pipeline-fastq-salivary"]
    found_one = False
    found_two = False
    for dir_ in dirs:
        for num in ["1", "2"]:
            key = "{}/{}/{}_{}.fastq.gz".format(dir_, sra_accession, sra_accession, num)
            if object_exists_in_s3(key):
                sh.aws("cp", "s3://{}/{}".format(bucket, key), ".")
                if num == "1":
                    found_one = True
                else:
                    found_two = True
    if found_one and found_two:
        return True
    return False


def object_exists_in_s3(key):
    "check if object exists in S3"
    try:
        sh.aws("s3api", "head-object", "--bucket", "fh-div-adm-scicomp", "--key", key)
        return True
    except sh.ErrorReturnCode_255:
        return False
    return False  # TODO revisit


def get_size_of_sra(sra_accession):
    "get size of sra"
    print("size of {} is {}.".format(sra_accession, sh.prefetch("-s", sra_accession)))


def download_from_sra(sra_accession):
    "download from sra"
    get_size_of_sra(sra_accession)
    minutes_to_sleep = random.randint(1, 60)
    print(
        "about to sleep for {} minutes to avoid slamming SRA".format(minutes_to_sleep)
    )
    time.sleep(minutes_to_sleep * 60)
    print("Downloading {} from sra...".format(sra_accession))
    if os.path.exists("{}/ncbi/dbGaP-17102/sra/{}.sra".format(HOME, sra_accession)):
        print("SRA file already exists, skipping download")
    else:
        prefetch = sh.prefetch(
            "--transport",
            "http",
            "--max-size",
            "100000000000",
            sra_accession,
            _iter=True,
            _err_to_out=True,
        )
        print("Beginning download...")
        for line in prefetch:
            print(line)
        prefetch_exit_code = prefetch.exit_code
        if prefetch_exit_code != 0:
            print(
                "prefetch existed with nonzero result-code {}, cleaning up and existing...".format(
                    prefetch_exit_code
                )
            )
            sh.rm("-rf", "{}/ncbi/dbGaP-17102/sra/{}.sra".format(HOME, sra_accession))
            for item in ["sra", "refseq"]:
                path = "{}/ncbi/public/{}/*".format(HOME, item)
                sh.rm("-rf", glob.glob(path))
            sys.exit(prefetch_exit_code)


def run_fastq_dump(sra_accession):
    "run fastq-dump"
    print("running fastq-dump...")

    # echo running fastq-dump
    # time parallel-fastq-dump --sra-id sra/$SRA_ACCESSION.sra --threads $NUM_CORES --outdir . --gzip --split-files -W -I --tmpdir $PTMP
    pfd = sh.parallel_fastq_dump(
        "--sra-id",
        "sra/{}.sra".format(sra_accession),
        "--threads",
        os.getenv("NUM_CORES"),
        "--gzip",
        "--split-files",
        "-W",
        "-I",
        "--tmpdir",
        PTMP,
        _iter=True,
        _err_to_out=True,
    )
    start = datetime.datetime.now()
    for line in pfd:
        print(line)

    end = datetime.datetime.now()

    print("duration of fastq-dump: {}".format(end - start))


def copy_fastqs_to_s3(sra_accession):
    "copy fastqs to s3"
    pass


def main():
    "do the work"
    ensure_correct_environment()
    print("public hostname for this container is {}".format(get_metadata()))
    print("container_id is {}".format(get_container_id))
    configure_aws()
    scratch, sra_accession = setup_scratch()
    with working_directory(Path("{}/ncbi/dbGaP-17102".format(HOME))):
        sh.mkdir("-p", PTMP)
        sh.rm("-rf", glob.glob("{}/*".format(PTMP)))
        print("sra accession is {}".format(sra_accession))
        print("scratch is {}".format(scratch))
    if not get_fastq_files_from_s3(sra_accession):
        download_from_sra(sra_accession)
        run_fastq_dump(sra_accession)
        copy_fastqs_to_s3(sra_accession)

    # run bowtie2 - stream to s3


if __name__ == "__main__":
    main()
