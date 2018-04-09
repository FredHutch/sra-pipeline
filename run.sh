#!/bin/bash

set -e # exit on error
set -o pipefail

# cd ~/ncbi/dbGaP-0/sra


if [[ -v AWS_BATCH_JOB_ID ]]
then
    echo this is an aws batch job
    rm -rf ~/ncbi
    aws s3 cp $ACCESSION_LIST /tmp/accessionlist.txt
    if [[ -v AWS_BATCH_JOB_ARRAY_INDEX ]]
    then
        echo this is an array job
        line="$((LN + 1))"
        SRA_ACCESSION=$(sed "${line}q;d" /tmp/accessionlist.txt)
        scratch=/scratch/$AWS_BATCH_JOB_ID/$AWS_BATCH_JOB_ARRAY_INDEX/
    else
        echo this is not an array job
        SRA_ACCESSION=$(sed '1q;d' /tmp/accessionlist.txt)
        scratch=/scratch/$AWS_BATCH_JOB_ID/
    fi
    mkdir -p $scratch
    ln -s $scratch ~/ncbi
    mkdir  ~/ncbi/dbGaP-0
else
    echo this is not an aws batch job
    SRA_ACCESSION=$(sed '1q;d' /tmp/accessionlist.txt)
    scratch=.
    mkdir -p ~/ncbi/dbGaP-0
fi

cd ~/ncbi/dbGaP-0


echo SRA_ACCESSION is $SRA_ACCESSION

echo scratch is $scratch



echo downloading $SRA_ACCESSION from sra...
prefetch --max-size 100000000000 $SRA_ACCESSION

echo done downloading.

# ( downloads to ~/ncbi/public/sra/)

viruses=( hhv6a hhv6b ) # TODO add another virus (and bt2 files)

echo starting pipeline...

for virus in "${viruses[@]}"; do
  echo processing $virus ...
  fastq-dump -Z ~/ncbi/public/sra/$SRA_ACCESSION.sra | bowtie2 -x /bt2/$virus - 2> >(tee stderr.log) | gzip -9  > $SRA_ACCESSION.sam.gz
  if grep -Fq "(100.00%) aligned 0 times" nomatch; then
     echo not found: virus $virus does not occur in $SRA_ACCESSION
  else
    echo found a match: virus $virus occurs in $SRA_ACCESSION - uploading to S3
    aws s3 cp $SRA_ACCESSION.sam.gz s3://$BUCKET_NAME/$PREFIX/$SRA_ACCESSION/$virus/$SRA_ACCESSION.sam.gz
  fi
  rm $SRA_ACCESSION.sam.gz

done




echo done with pipeline, cleaning up

if [[ -v AWS_BATCH_JOB_ID ]]
then
    rm -rf $scratch
fi


echo exiting...
