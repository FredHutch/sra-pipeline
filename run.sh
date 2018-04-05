#!/bin/bash

set -e # exit on error
set -o pipefail

# cd ~/ncbi/dbGaP-0/sra


# TODO uncomment when running in batch using an AMI that has scratch space at /scratch
# rm -rf ~/ncbi
# $scratch=/scratch/$AWS_BATCH_JOB_ID/$AWS_BATCH_JOB_ARRAY_INDEX/
# mkdir -p $scratch
# ln -s $scratch ~/ncbi
# mkdir  ~/ncbi/dbGaP-0
# cd ncbi

cd ~/ncbi/dbGaP-0



# TODO - translate batch array index into SRA file name, put into $SRA_FILE.

echo downloading $SRA_ACCESSION from sra...
prefetch --max-size 100000000000 $SRA_ACCESSION

echo done downloading.

# ( downloads to ~/ncbi/public/sra/)

viruses=( hhv6a hhv6b ) # TODO add another virus (and bt2 files)

echo starting pipeline...

for virus in "${viruses[@]}"; do
  echo processing $virus ...
  fastq-dump -Z ~/ncbi/public/sra/$SRA_ACCESSION.sra | bowtie2 -x /bt2/$virus - | gzip -9 | aws s3 cp - s3://$BUCKET_NAME/$PREFIX/$SRA_ACCESSION/$virus/$SRA_ACCESSION.sam.gz

done




echo done with pipeline, cleaning up

# TODO uncomment when running in batch....
# rm -rf $scratch

echo exiting...
