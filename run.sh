#!/bin/bash

set -e # exit on error
set -o pipefail

# cd ~/ncbi/dbGaP-0/sra

mkdir -p /tmp/al

if [[ -v AWS_BATCH_JOB_ID ]]
then
    echo this is an aws batch job
    rm -rf ~/ncbi
    aws s3 cp $ACCESSION_LIST /tmp/al/accessionlist.txt
    if [[ -v AWS_BATCH_JOB_ARRAY_INDEX ]]
    then
        echo this is an array job
        line="$((LN + 1))"
        SRA_ACCESSION=$(sed "${line}q;d" /tmp/al/accessionlist.txt)
        scratch=/scratch/$AWS_BATCH_JOB_ID/$AWS_BATCH_JOB_ARRAY_INDEX/
    else
        echo this is not an array job
        SRA_ACCESSION=$(sed '1q;d' /tmp/al/accessionlist.txt)
        scratch=/scratch/$AWS_BATCH_JOB_ID/
    fi
    mkdir -p $scratch
    ln -s $scratch ~/ncbi
    mkdir  ~/ncbi/dbGaP-0
else
    echo this is not an aws batch job
    SRA_ACCESSION=$(sed '1q;d' /tmp/al/accessionlist.txt)
    scratch=.
    mkdir -p ~/ncbi/dbGaP-0
fi

cd ~/ncbi/dbGaP-17102


echo SRA_ACCESSION is $SRA_ACCESSION

echo scratch is $scratch



echo downloading $SRA_ACCESSION from sra...
prefetch --max-size 100000000000 --transport ascp --ascp-options "-l 100M" $SRA_ACCESSION
echo done downloading.

# ( downloads to ~/ncbi/public/sra/)

viruses=( hhv6a hhv6b hhv-7 )

echo starting pipeline...

for virus in "${viruses[@]}"; do
  echo processing $virus ...
  time (fastq-dump -Z ~/ncbi/public/sra/$SRA_ACCESSION.sra | bowtie2 -x /bt2/$virus - 2> >(tee stderr.log) | gzip -9 | aws s3 cp - s3://$BUCKET_NAME/$PREFIX/$SRA_ACCESSION/$virus/$SRA_ACCESSION.sam.gz )
done




echo done with pipeline, cleaning up

if [[ -v AWS_BATCH_JOB_ID ]]
then
    rm -rf $scratch
fi


echo exiting...
