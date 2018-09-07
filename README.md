# SRA Pipeline

This repository contains code for running an analysis pipeline in
[AWS Batch](https://aws.amazon.com/batch/).

## What the pipeline does

Given a Synapse ID pointing to a collection of BAM files, AWS Batch will start an
[array job](https://docs.aws.amazon.com/batch/latest/userguide/array_jobs.html)
where each child will process a single bam file, doing the following:

* Download the BAM associated with the synapse ID
* Using `samtools`, convert the BAM file to fastq
* 
* Run [bowtie2](http://bowtie-bio.sourceforge.net/bowtie2/index.shtml)
     to search for the virus.
   * Pipe the output of `bowtie2` through
     [gzip](https://en.wikipedia.org/wiki/Gzip) to compress it prior to
     the next step.
   * stream the compressed output of `bowtie2` to an
     [S3](https://aws.amazon.com/s3/) bucket. The resulting file will
     have an S3 URL like this: `s3://<bucket-name>/<prefix>/<SRA-accession-number>/<virus>/<SRA-accession-number>.sam.gz`.


## Prerequisites/Requirements

* These tools must all be run on the Fred Hutch internal network.
* Obtain your S3 credentials using the
  [awscreds](https://teams.fhcrc.org/sites/citwiki/SciComp/Pages/Getting%20AWS%20Credentials.aspx)
  script. You only need to do this once.
* [Request access](https://fredhutch.github.io/aws-batch-at-hutch-docs/#request-access)
  to AWS Batch.
* Clone this repository to a location under your home directory, and then
  change directories into the repository (you only need to do this once,
  although you may need to run `git pull` periodically to keep
  your cloned repository up to date):

```
git clone https://github.com/FredHutch/sra-pipeline.git
cd sra-pipeline
```


## `sra_pipeline` utility

A script called `sra_pipeline` is available to 
facilitate job submission. 
Use the `-s` option to specify a Synapse ID for 
a synapse resource containing BAM files.

An example run might look like this:

```
./sra_pipeline -p myprefix -y hhv6a_u1102_untrimmed,hhv6b_z29_untrimmed,t_ref -s syn4645334
```

This will process all BAM files referenced by the
synapse ID `syn4645334`, search them for
hhv6a untrimmed, hhv6b untrimmed, and t_ref, and
put the results in the `myprefix` prefix in S3.

Running the utility with `--help` gives usage information:

```
$ ./sra_pipeline --help
usage: sra_pipeline.py [-h] [-c JOB_ID] [-i JOB_ID] [-r JOB_ID] [-f FILE]
                       [-p PREFIX] [-q STR] [-y REFERENCES] [-s SYNAPSE_ID]
                       [job_id]

positional arguments:
  job_id                a job ID to search the logs of (use with -q only)
                        (default: None)

optional arguments:
  -h, --help            show this help message and exit
  -c JOB_ID, --completed JOB_ID
                        show completed accession numbers (default: None)
  -i JOB_ID, --in-progress JOB_ID
                        show accession numbers that are in progress (default:
                        None)
  -r JOB_ID, --remaining JOB_ID
                        show remaining items (not yet completed) (default:
                        None)
  -f FILE, --submit-file FILE
                        submit accession numbers contained in FILE (default:
                        None)
  -p PREFIX, --prefix PREFIX
                        override default prefix (default: pipeline-results)
  -q STR, --query STR   string to search for in logs, must specify JOB_ID
                        (default: finished downloading)
  -y REFERENCES, --references REFERENCES
                        comma-separated list of references (default: None)
  -s SYNAPSE_ID, --synapse-id SYNAPSE_ID
                        run against all bam files listed under SYNAPSE_ID
                        (default: None)
```


## Additional monitoring of jobs

You can get more detail about running jobs by using  
the [Batch Dashboard](https://batch-dashboard.fhcrc.org/)
and/or the
[AWS command-line client for Batch](https://docs.aws.amazon.com/cli/latest/reference/batch/index.html).

See [Using AWS Batch at Fred Hutch](https://fredhutch.github.io/aws-batch-at-hutch-docs/)
for more information.
