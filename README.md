# Nextflow version of the NIPT pipeline

## Prerequisites

### Get a tower token

Go to [https://tower.nf/login](https://tower.nf/login). Enter your email.
The first time you do this, your request will go to a human who will eventually
send you a tower token. Keep this in a safe place.

Tower is a service that provides graphical real-time monitoring of nextflow jobs.

### Get Python 3, pip, and pipenv

If you don't already have it, install python 3.

If you don't already have it, install pip (for python 3).

Install pipenv:

```
pip3 install pipenv
```


### Clone two repositories

First, clone this repository and switch to this branch:

```
git clone https://github.com/FredHutch/sra-pipeline.git
cd sra-pipeline
git checkout nextflow
```

Now go up one level:

```
cd ..
```


Now clone the repository that lets you run "batch squared" jobs:

```
git clone https://github.com/FredHutch/nextflow-aws-batch-squared.git
cd aws-batch-squared
```

#### Set up the virtual environment

First time only, install dependencies:

```
pipenv install
```

Each subsequent time you want to kick off the job, run this command 
to switch to the virtual environment:

```
pipenv shell 
```

## Starting a job

Put all the samples you want to
process into `s3://fh-pi-jerome-k/nipt_pipeline/all_fastqs/`.


If you have a tower token, export it:

```
export TOWER_TOKEN=some-value
```

(`some-value` is just an example; use your actual tower token here.)

Run the command:

```
../sra-pipeline/submit.sh
```

Once you see a line that looks like this:

```
2019-10-11 12:46:38,765 INFO     [Nextflow AWS Batch Squared] Started nipt-pipeline-headnode-0 as AWS Batch ID c4cad3ec-db28-438b-83fb-2ee555169aff (unique Nextflow ID: c7cf67e9-5910-4b19-a75a-4edf780a38c8)
```

...your job has started, and it is OK to press control-c or turn off your computer.


If you have a tower token, you can monitor your job
at [https://tower.nf](https://tower.nf).


