#!/usr/bin/python3.6

"script to run on AWS batch instance"

import contextlib
import csv
import datetime
from functools import partial
import glob
import json
import os
import os.path
from pathlib import Path
import random
import re
import sys
import time
import traceback

import sh
import requests

HOME = os.getenv("HOME")
PTMP = "tmp"


class Timer:  # pylint: disable=too-few-public-methods
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


def get_synapse_metadata(batch_job_array_index):
    """
    given a line number, get synapse metadata for that line
    Line number (starts from 1, not 0) should take into account header. So if we want the
    first non header line, it should be 2.

    Returns the line converted to a dict where the keys are
    from the header line of the tsv.
    """
    if os.path.exists("temp.tsv"):
        os.remove("temp.tsv")
    with open("temp.tsv", "a") as tmpfile:
        sh.head("-1", "accessionlist.txt", _out=tmpfile)
        line = batch_job_array_index
        sh.sed("{}q;d".format(line), "accessionlist.txt", _out=tmpfile)
    hsh = None
    with open("temp.tsv") as tmpfile_r:
        reader = csv.DictReader(tmpfile_r, delimiter="\t")
        for row in reader:
            hsh = row
    return dict(hsh)


def setup_scratch():
    "sets up scratch, returns scratch dir and sra accession number"
    sh.aws("s3", "cp", os.getenv("ACCESSION_LIST"), "accessionlist.txt")
    if os.getenv("AWS_BATCH_JOB_ID"):
        fprint("this is a batch job")
        sh.rm("-rf", "{}/ncbi".format(HOME))
        if os.getenv("AWS_BATCH_JOB_ARRAY_INDEX"):
            fprint("this is an array job")
            # add 2, one because AWS counts from 0 and sed counts from 1,
            # and one because of the header line.
            line = int(os.getenv("AWS_BATCH_JOB_ARRAY_INDEX")) + 2
            synapse_metadata = get_synapse_metadata(line)
            scratch = "/scratch/{}/{}/".format(
                os.getenv("AWS_BATCH_JOB_ID"), os.getenv("AWS_BATCH_JOB_ARRAY_INDEX")
            )
        else:
            fprint("this is not an array job")
            synapse_metadata = get_synapse_metadata(2)
            scratch = "/scratch/{}/".format(os.getenv("AWS_BATCH_JOB_ID"))
        sh.mkdir("-p", scratch)
        sh.ln("-s", scratch, "{}/ncbi".format(HOME))
        sh.mkdir("-p", "{}/ncbi/dbGaP-17102".format(HOME))
    else:
        fprint("this is not an aws batch job")
        synapse_metadata = get_synapse_metadata(2)
        scratch = "."
        sh.mkdir("-p", "{}/ncbi/dbGaP-17102".format(HOME))
    return scratch, synapse_metadata


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


def run_fastq_dump(sra_accession):
    "run fastq-dump"
    fprint("running fastq-dump...")

    # pfd0 = sh.Command("/home/neo/miniconda3/bin/parallel-fastq-dump")
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


def run_bowtie(synapse_id, fastq_file_name):
    """
    run bowtie2
    synapse_id - synapse id
    fastq_file_name - fastq file name
    """
    viruses = os.getenv("REFERENCES").split(",")
    viruses = [x.strip() for x in viruses]
    bowtie2 = partial(sh.bowtie2, _piped=True, _bg_exc=False, _err="bowtie2.err")

    for virus in viruses:
        bowtie_args = [
            "--local",
            "-p",
            os.getenv("NUM_CORES"),
            "--no-unal",
            "-x",
            "/bt2/{}".format(virus),
            "-U",
            fastq_file_name,
        ]
        fprint("processing virus {} ...".format(virus))
        if object_exists_in_s3(
            "{}/{}/{}/{}.sam".format(os.getenv("PREFIX"), synapse_id, virus, synapse_id)
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
                        synapse_id,
                        virus,
                        synapse_id,
                    ),
                    _iter=True,
                ):
                    fprint(line)
            fprint("bowtie2 duration for {}: {}".format(virus, timer.interval))
            fprint("stderr output of bowtie2:")
            for line in sh.cat("bowtie2.err", _iter=True):
                fprint(line)
            sh.aws(
                "s3",
                "cp",
                "bowtie2.err",
                "s3://{}/{}/{}/{}/".format(
                    os.getenv("BUCKET_NAME"), os.getenv("PREFIX"), synapse_id, virus
                ),
            )


def get_read_counts(sra_accession):
    "return read counts for fastq files 1 and 2"
    results = []
    for i in range(1, 3):
        result = int(
            sh.awk(
                sh.zcat("{}_{}.fastq.gz".format(sra_accession, i)),
                "{s++}END{print s/4}",
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


def add_to_path(directory):
    """
    add a directory to the PATH since the script seems to forget
    what's in the PATH sometimes.
    """
    path = "{}:{}".format(os.getenv("PATH"), directory)
    os.environ["PATH"] = path
    print("Added {} to PATH.".format(directory))


def download_from_synapse(synapse_id, bam_file_name):
    "download fastq file from synapse"
    # TODO FIXME think about piping/streaming to bowtie2?
    if os.path.exists(bam_file_name):
        os.remove(bam_file_name)
    sh.synapse("get", synapse_id)


def convert_bam_to_fastq(bam_file_name):
    "convert bam to fastq"
    fastq_file_name = re.sub(".bam$", ".fastq.gz", bam_file_name)
    sh.gzip(
        sh.samtools(
            sh.samtools("view", "-b", "-f", "4", bam_file_name, _piped=True),
            "bam2fq",
            "-",
            _piped=True,
        ),
        "-f",
        _out=fastq_file_name,
    )
    return fastq_file_name


def main():
    "do the work"
    ensure_correct_environment()
    add_to_path("/home/neo/miniconda3/bin")
    add_to_path("/bowtie2-2.3.4.1-linux-x86_64")
    add_to_path("/sratoolkit.2.9.2-ubuntu64/bin")
    fprint("public hostname for this container is {}".format(get_metadata()))
    fprint("container_id is {}".format(get_container_id()))
    configure_aws()
    # get ngc file from s3
    sh.aws("s3", "cp", "s3://fh-pi-jerome-k/pipeline-auth-files/prj_17102.ngc", ".")
    sh.vdb_config("--import", "prj_17102.ngc")
    # get synapse auth file from s3
    sh.aws(
        "s3",
        "cp",
        "s3://fh-pi-jerome-k/pipeline-auth-files/.synapseConfig",
        "{}/".format(HOME),
    )
    scratch, synapse_metadata = setup_scratch()
    with working_directory(Path("{}/ncbi/dbGaP-17102".format(HOME))):
        sh.mkdir("-p", PTMP)
        clean_directory(PTMP)

        synapse_id = synapse_metadata["file.id"]
        bam_file_name = synapse_metadata["file.name"]

        fprint("synapse id is", synapse_id)
        fprint("bam (?) file name is", bam_file_name)
        fprint("scratch is {}".format(scratch))

        download_from_synapse(synapse_id, bam_file_name)
        # dante TODO FIXME ...
        # if not get_fastq_files_from_s3(sra_accession):
        #     download_from_sra(sra_accession)
        #     run_fastq_dump(sra_accession)
        #     copy_fastqs_to_s3(sra_accession)
        fastq_file_name = convert_bam_to_fastq(bam_file_name)
        try:
            run_bowtie(synapse_id, fastq_file_name)
        except:  # pylint: disable=bare-except
            fprint("Unexpected exception:")
            fprint(traceback.print_exception(*sys.exc_info()))
            sys.exit(1)
        finally:  # hopefully we still exit with an error code if there was an error
            cleanup(scratch)


if __name__ == "__main__":
    main()
