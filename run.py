#!/usr/bin/python3.6

"script to run on AWS batch instance"

import contextlib
import glob
import importlib.util
import json
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


def fprint(*args, **kwargs):
    """
    print and then flush stdout.
    """
    # TODO - print to log file as well?
    print(*args, **kwargs)
    sys.stdout.flush()


def get_metadata():
    "get ec2 metadata if available"
    try:
        return requests.get(
            "http://169.254.169.254/latest/meta-data/public-hostname", timeout=1
        ).text.strip()
    except requests.exceptions.Timeout:
        return "unknown"


def get_container_id():
    "get container id"
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


def setup_scratch():
    "sets up scratch, returns scratch dir and sra accession number"
    sh.aws("s3", "cp", os.getenv("ACCESSION_LIST"), "accessionlist.txt")
    if os.getenv("AWS_BATCH_JOB_ID"):
        fprint("this is a batch job")
        sh.rm("-rf", "{}/ncbi".format(HOME))
        if os.getenv("AWS_BATCH_JOB_ARRAY_INDEX"):
            fprint("this is an array job")
            scratch = "/scratch/{}".format(
                os.getenv("AWS_BATCH_JOB_ID").replace(":", "_")
            )
        else:
            fprint("this is not an array job")
            scratch = "/scratch/{}/".format(os.getenv("AWS_BATCH_JOB_ID"))
        sh.mkdir("-p", scratch)
        sh.ln("-s", scratch, "{}/ncbi".format(HOME))
        sh.mkdir("-p", "{}/ncbi/dbGaP-17102".format(HOME))
    else:
        fprint("this is not an aws batch job")
        scratch = "."
        sh.mkdir("-p", "{}/ncbi/dbGaP-17102".format(HOME))
    return scratch


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
                fprint("Downloading {}_{}.fastq.gz....".format(sra_accession, num))
                sh.aws("s3", "cp", "s3://{}/{}".format(bucket, key), ".")
                # false positive below:
                # https://github.com/PyCQA/pylint/issues/837#issuecomment-255109936
                if num == "1":  # pylint: disable=simplifiable-if-statement
                    found_one = True
                else:
                    found_two = True
        if found_one and found_two:
            return True
    return False


def object_exists_in_s3(key):
    "check if object exists in S3 and is not empty"
    try:
        ret = sh.aws(
            "s3api", "head-object", "--bucket", os.getenv("BUCKET_NAME"), "--key", key
        )
        obj = json.loads(str(ret))
        return obj["ContentLength"] > 0
    except sh.ErrorReturnCode_255:
        return False
    return False  # TODO revisit


def get_size_of_sra(sra_accession):
    "get size of sra"
    # prefetch = sh.Command("/sratoolkit.2.9.2-ubuntu64/bin/prefetch")
    fprint("size of {} is {}.".format(sra_accession, sh.prefetch("-s", sra_accession)))


def download_from_sra(sra_accession):
    "download from sra"
    get_size_of_sra(sra_accession)
    if not os.getenv("DISABLE_SLEEP"):
        minutes_to_sleep = random.randint(1, 60)
        fprint(
            "about to sleep for {} minutes to avoid slamming SRA".format(
                minutes_to_sleep
            )
        )
        time.sleep(minutes_to_sleep * 60)
    fprint("Downloading {} from sra...".format(sra_accession))
    if os.path.exists("{}/ncbi/dbGaP-17102/sra/{}.sra".format(HOME, sra_accession)):
        fprint("SRA file already exists, skipping download")
    else:
        # prefetch_cmd = sh.Command("/sratoolkit.2.9.2-ubuntu64/bin/prefetch")
        prefetch = sh.prefetch(
            "--transport",
            "http",
            "--max-size",
            "100000000000",
            sra_accession,
            _iter=True,
            _err_to_out=True,
        )
        fprint("Beginning download...")
        for line in prefetch:
            fprint(line)
        prefetch_exit_code = prefetch.exit_code
        if prefetch_exit_code != 0:
            fprint(
                "prefetch exited with nonzero result-code {}, cleaning up and exiting...".format(
                    prefetch_exit_code
                )
            )
            sh.rm("-rf", "{}/ncbi/dbGaP-17102/sra/{}.sra".format(HOME, sra_accession))
            for item in ["sra", "refseq"]:
                clean_directory("{}/ncbi/public/{}".format(HOME, item))
            sys.exit(prefetch_exit_code)


def cleanup(scratch):
    "clean up"
    fprint("done with pipeline, cleaning up")
    if os.getenv("AWS_BATCH_JOB_ID"):
        sh.rm("-rf", scratch)


def clean_directory(dirname):
    """
    Remove all files from `dirname` without removing the directory itself.
    In the shell you can say `rm -rf dirname/*` and it will do the right thing,
    it's a bit trickier with sh and glob.
    """
    globb = glob.glob("{}/**".format(dirname), recursive=True)
    globb.remove("{}/".format(dirname))  # we don't want to remove the directory itself
    sh.rm("-rf", globb)


def add_to_path(directory):
    """
    add a directory to the PATH since the script seems to forget
    what's in the PATH sometimes.
    """
    path = "{}:{}".format(os.getenv("PATH"), directory)
    os.environ["PATH"] = path
    print("Added {} to PATH.".format(directory))


def main():
    "do the work"
    add_to_path("/home/neo/miniconda3/bin")
    add_to_path("/bowtie2-2.3.4.1-linux-x86_64")
    add_to_path("/sratoolkit.2.9.2-ubuntu64/bin")
    fprint("public hostname for this container is {}".format(get_metadata()))
    fprint("container_id is {}".format(get_container_id()))
    configure_aws()

    scratch = setup_scratch()

    scratch = os.path.join(scratch, "sra-pipeline-clone")
    sh.git("clone", "https://github.com/FredHutch/sra-pipeline.git", scratch)

    # with working_directory(Path("{}/ncbi/dbGaP-17102".format(HOME))):
    with working_directory(Path(scratch)):
        sh.git("checkout", os.getenv("GIT_BRANCH"))

        # sh.mkdir("-p", PTMP)
        # clean_directory(PTMP)

        index = int(os.getenv("AWS_BATCH_JOB_ARRAY_INDEX"))

        spec = importlib.util.spec_from_file_location(
            "get_num_pairs", "get_num_pairs.py"
        )
        get_num_pairs = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(get_num_pairs)


        fastq_pair_name = get_num_pairs.get_unfinished_pairs()[index]
        bucket = os.getenv("S3_BUCKET")

        # TODO check S3 for output file; if it already exists, exit

        for fnum in range(1, 3):
            sh.aws(
                "s3",
                "cp",
                "s3://{}/{}.{}.fastq.gz".format(bucket, fastq_pair_name, fnum),
                ".",
            )
        sh.mkdir("indexes")
        sh.aws("s3", "cp", os.getenv("REFERENCE_LOCATION"), "./indexes/", "--recursive")

        references = os.getenv("REFERENCES").split(",")
        filename = fastq_pair_name.split("/")[-1]
        for ref in references:
            sh.python3("nipt_pipeline.py", filename, ref)
            sh.aws(
                "s3",
                "cp",
                "{}.sam".format(filename),
                "{}/{}/".format(os.getenv("OUTPUT_LOCATION"), ref),
            )

    cleanup(scratch)


if __name__ == "__main__":
    main()
