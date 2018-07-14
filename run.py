#!/usr/bin/env python3

"script to run on AWS batch instance"

import contextlib
import datetime
from functools import partial
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


class Timer:
    "tweaked from http://preshing.com/20110924/timing-your-code-using-pythons-with-statement/"

    def __init__(self):
        self.start = None
        self.end = None
        self.interval = None

    def __enter__(self):
        self.start = datetime.datetime.now()
        return self

    def __exit__(self, *args):
        self.end = datetime.datetime.now()
        self.interval = self.end - self.start


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


def ensure_correct_environment():
    "ensure correct environment"
    if not os.getenv("NUM_CORES"):
        fprint("NUM_CORES is not set, exiting")
        sys.exit(1)


def setup_scratch():
    "sets up scratch, returns scratch dir and sra accession number"
    sh.aws("s3", "cp", os.getenv("ACCESSION_LIST"), "accessionlist.txt")
    if os.getenv("AWS_BATCH_JOB_ID"):
        fprint("this is a batch job")
        sh.rm("-rf", "{}/ncbi".format(HOME))
        if os.getenv("AWS_BATCH_JOB_ARRAY_INDEX"):
            fprint("this is an array job")
            line = int(os.getenv("AWS_BATCH_JOB_ARRAY_INDEX")) + 1
            sra_accession = sh.sed("{}q;d".format(line), "accessionlist.txt").strip()
            scratch = "/scratch/{}/{}/".format(
                os.getenv("AWS_BATCH_JOB_ID"), os.getenv("AWS_BATCH_JOB_ARRAY_INDEX")
            )
        else:
            fprint("this is not an array job")
            sra_accession = sh.sed("1q;d", "accessionlist.txt").strip()
            scratch = "/scratch/{}/".format(os.getenv("AWS_BATCH_JOB_ID"))
        sh.mkdir("-p", scratch)
        sh.ln("-s", scratch, "{}/ncbi".format(HOME))
        sh.mkdir("-p", "{}/ncbi/dbGaP-17102".format(HOME))
    else:
        fprint("this is not an aws batch job")
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
                sh.aws("s3", "cp", "s3://{}/{}".format(bucket, key), ".")
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
        sh.aws(
            "s3api", "head-object", "--bucket", os.getenv("BUCKET_NAME"), "--key", key
        )
        return True
    except sh.ErrorReturnCode_255:
        return False
    return False  # TODO revisit


def get_size_of_sra(sra_accession):
    "get size of sra"
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
                "prefetch existed with nonzero result-code {}, cleaning up and exiting...".format(
                    prefetch_exit_code
                )
            )
            sh.rm("-rf", "{}/ncbi/dbGaP-17102/sra/{}.sra".format(HOME, sra_accession))
            for item in ["sra", "refseq"]:
                clean_directory("{}/ncbi/public/{}".format(HOME, item))
            sys.exit(prefetch_exit_code)


def run_fastq_dump(sra_accession):
    "run fastq-dump"
    fprint("running fastq-dump...")

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
    with Timer() as timer:
        for line in pfd:
            fprint(line)

    fprint("duration of fastq-dump: {}".format(timer.interval))


def copy_fastqs_to_s3(sra_accession):
    "copy fastqs to s3"
    for i in range(1, 3):
        sh.aws(
            "s3",
            "cp",
            "{}_{}.fastq.gz".format(sra_accession, i),
            "s3://{}/pipeline-fastq/{}/".format(
                os.getenv("BUCKET_NAME"), sra_accession
            ),
        )


def run_bowtie(sra_accession, read_handling="equal"):
    """
    run bowtie2
    sra_accession - sra accession
    read_handling - if both fastq files are of equal length
                    (indicated by value "equal", the default),
                    then both fastq files are used. If value is
                    1 or 2, then the given single fastq file is used.
    """
    viruses = os.getenv("REFERENCES").split(",")
    viruses = [x.strip() for x in viruses]
    bowtie2 = partial(sh.bowtie2, _piped=True)

    for virus in viruses:
        bowtie_args = [
            "--local",
            "--p",
            os.getenv("NUM_CORES"),
            "--no-unal",
            "-x",
            "/bt2/{}".format(virus),
        ]
        if read_handling == "equal":
            bowtie_args.extend(
                [
                    "-1",
                    "{}_1.fastq.gz".format(sra_accession),
                    "-2",
                    "{}_2.fastq.gz".format(sra_accession),
                ]
            )
        elif read_handling == 1:
            bowtie_args.extend(["-U", "{}_1.fastq.gz".format(sra_accession)])
        elif read_handling == 2:
            bowtie_args.extend(["-U", "{}_2.fastq.gz".format(sra_accession)])

        fprint("processing virus {} ...".format(virus))
        if object_exists_in_s3(
            "{}/{}/{}/{}.sam".format(
                os.getenv("PREFIX"), sra_accession, virus, sra_accession
            )
        ):
            fprint(
                "output sam file already exists in s3 for virus {}, skipping...".format(
                    virus
                )
            )
        else:
            with Timer() as timer:
                for line in sh.aws(
                    bowtie2(*bowtie_args),
                    "s3",
                    "cp",
                    "-",
                    "s3://{}/{}/{}/{}/{}.sam".format(
                        os.getenv("BUCKET_NAME"),
                        os.getenv("PREFIX"),
                        sra_accession,
                        virus,
                        sra_accession,
                    ),
                    _iter=True,
                ):
                    fprint(line)
            fprint("bowtie2 duration for {}: {}".format(virus, timer.interval))


def get_read_counts(sra_accession):
    "return read counts for fastq files 1 and 2"
    results = []
    for i in range(1, 3):
        result = int(
            sh.awk(
                sh.zcat("{}_{}.fastq.gz".format(sra_accession, i)),
                '{s++}END{print s/4}',
                _piped=True,
            ).strip()
        )
        results.append(result)
    return results[0], results[1]


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


def main():
    "do the work"
    ensure_correct_environment()
    fprint("public hostname for this container is {}".format(get_metadata()))
    fprint("container_id is {}".format(get_container_id()))
    configure_aws()
    scratch, sra_accession = setup_scratch()
    with working_directory(Path("{}/ncbi/dbGaP-17102".format(HOME))):
        sh.mkdir("-p", PTMP)
        clean_directory(PTMP)
        fprint("sra accession is {}".format(sra_accession))
        fprint("scratch is {}".format(scratch))
        if not get_fastq_files_from_s3(sra_accession):
            download_from_sra(sra_accession)
            run_fastq_dump(sra_accession)
            copy_fastqs_to_s3(sra_accession)

        rc1, rc2 = get_read_counts(sra_accession)
        if rc1 == rc2:
            run_bowtie(sra_accession)
        elif rc1 > rc2:
            run_bowtie(sra_accession, 1)
        elif rc2 > rc1:
            run_bowtie(sra_accession, 2)

        cleanup(scratch)


if __name__ == "__main__":
    main()
