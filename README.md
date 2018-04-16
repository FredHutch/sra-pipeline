# SRA Pipeline

This repository contains code for running an analysis pipeline in
[AWS Batch](https://aws.amazon.com/batch/).

## What the pipeline does

Given a set of SRA accession numbers, AWS Batch will start an
[array job](https://docs.aws.amazon.com/batch/latest/userguide/array_jobs.html)
where each child will process a single accession number, doing the following:

* Download the file(s) associated with the accession number from
  [SRA](https://www.ncbi.nlm.nih.gov/sra), using the
  [prefetch](https://trace.ncbi.nlm.nih.gov/Traces/sra/sra.cgi?view=toolkit_doc&f=prefetch)
  tool with the [Aspera Connect](http://downloads.asperasoft.com/connect2//) transport.
* Start a [bash](https://en.wikipedia.org/wiki/Bash_(Unix_shell)) pipe which
  runs the following steps, once for each of three viral genomes.
  * extracts the downloaded `.sra` file to `fastq` format using
    [fastq-dump](https://ncbi.github.io/sra-tools/fastq-dump.html). The sra
    file is highly compressed and this step can expand it to more than 20 times
    its size, which is one reason we stream the data in a pipe: so as to
    not need lots of scratch space.
 * Pipe the `fastq` data through
   [bowtie2](http://bowtie-bio.sourceforge.net/bowtie2/index.shtml)
   to search for the virus.
 * Pipe the output of `bowtie2` through
   [gzip](https://en.wikipedia.org/wiki/Gzip) to compress it prior to
   the next step.
 * stream the compressed output of `bowtie2` to an
   [S3](https://aws.amazon.com/s3/) bucket. The resulting file will
   have an S3 URL like this: `s3://<bucket-name>/pipeline-results/<SRA-accession-number>/<virus>/<SRA-accession-number>.sam.gz`.


## Prerequisites/Requirements

* Run all these tools on the Fred Hutch internal network.
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

A script called `sra_pipeline` is available to to simplify the following:

* Display accession numbers that have already been processed.
* Display accession numbers which are currently being processed.
* Submit some number of new accession numbers to the pipeline, choosing
  either randomly, or by picking the smallest available data sets.

Running the utility with `--help` gives usage information:

```
$ ./sra_pipeline --help
usage: sra_pipeline.py [-h] [-c] [-i] [-s N] [-r N]

optional arguments:
  -h, --help            show this help message and exit
  -c, --completed       show completed accession numbers
  -i, --in-progress     show accession numbers that are in progress
  -s N, --submit-small N
                        submit N jobs of ascending size
  -r N, --submit-random N
                        submit N randomly chosen jobs
```


## Additional monitoring of jobs

You can get more detail about running jobs by using  
the [Batch Dashboard](https://batch-dashboard.fhcrc.org/)
and/or the
[AWS command-line client for Batch](https://docs.aws.amazon.com/cli/latest/reference/batch/index.html).

See [Using AWS Batch at Fred Hutch](https://fredhutch.github.io/aws-batch-at-hutch-docs/)
for more information.
