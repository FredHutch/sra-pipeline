# SRA Pipeline

This repository contains code for running an analysis pipeline in
[AWS Batch](https://aws.amazon.com/batch/).

## What the pipeline does


First you kick off a single AWS Batch Array Job, of N elements,
where N is the number of pairs of fastq.gz files that can be
found under a specific bucket/prefix in S3.

The job also specifies other information, such as the location
of index (fasta) files in s3, and where the output files should go.
This is all specified in the `job.json` file.

Each child job will download a single pair of fastq files
(based on the index it is given, available as the 
environment variable `AWS_BATCH_JOB_ARRAY_INDEX`).

It will also download the index fasta files and convert them
(using `bowtie2-build`) to several `.bt2` files.

Then, for each of the references we are looking for,
it will run `nipt_pipeline.py` which runs `bowtie2` to
align the fastq files against the reference.

The output goes into a `.sam` file which is uploaded to S3 
under a prefix that indicates which reference was used.


## Prerequisites

You need to install python 3 and `pipenv` on your local machine.

On a Mac, you can get the latest python 3 here:

https://www.python.org/downloads/mac-osx/

Note that installing python 3 will not affect the existing python 2 installation.
The python 3 executable is called `python3` and the existing `python` executable is left alone.

To install `pipenv`, run:

```
pip3 install --user pipenv
```

Then clone this repository if you haven't already:

```
git clone https://github.com/FredHutch/sra-pipeline.git
cd sra-pipeline
```

### Starting the job

First make sure that you have checked out the correct branch of code and that 
you have the latest changes:

```
git checkout feature/fastq-files-from-s3
git pull
```



First, determine how many pairs there are by running this script:

```
python get_num_pairs.py
```

Assuming the upload is done, that will give you a number. You can then paste that
number into line 5 of `job.json` to give the size of the array job.

Now you can submit the job like this:


```
aws batch submit-job --cli-input-json file://job.json
```

This command will give you a job ID which you should hold on to for monitoring (see next section).

The output files will end up in the location specified as `OUTPUT_LOCATION` in the `job.json` file.


## Additional monitoring of jobs

You can get more detail about running jobs by using  
the [Batch Dashboard](https://batch-dashboard.fhcrc.org/)
and/or the
[AWS command-line client for Batch](https://docs.aws.amazon.com/cli/latest/reference/batch/index.html).

See [Using AWS Batch at Fred Hutch](https://fredhutch.github.io/aws-batch-at-hutch-docs/)
for more information.
