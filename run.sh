#!/bin/bash

set -o pipefail

set -e



# send all output to a file as well (see closing brace at the bottom)
{

set -e # exit on error
set -o pipefail
set -x

public_hostname=$(curl -s http://169.254.169.254/latest/meta-data/public-hostname || true)
echo public hostname for this container is $public_hostname
container_id=$(cat /proc/self/cgroup | head -n 1 | cut -d '/' -f3)
echo container id is $container_id

aws s3 cp $ACCESSION_LIST accessionlist.txt

aws configure set default.s3.multipart_chunksize 50MB
aws configure set default.s3.max_concurrent_requests 100
aws configure set default.s3.max_queue_size 10000
aws configure set default.s3.multipart_threshold 64MB




if [[ -v AWS_BATCH_JOB_ID ]]
then
    echo this is an aws batch job
    rm -rf ~/ncbi
    if [[ -v AWS_BATCH_JOB_ARRAY_INDEX ]]
    then
        echo this is an array job
        line="$((AWS_BATCH_JOB_ARRAY_INDEX + 1))"
        SRA_ACCESSION=$(sed "${line}q;d" accessionlist.txt)
        scratch=/scratch/$AWS_BATCH_JOB_ID/$AWS_BATCH_JOB_ARRAY_INDEX/
    else
        echo this is not an array job
        SRA_ACCESSION=$(sed '1q;d' accessionlist.txt)
        scratch=/scratch/$AWS_BATCH_JOB_ID/
    fi
    mkdir -p $scratch
    ln -s $scratch ~/ncbi
    mkdir  -p ~/ncbi/dbGaP-17102
else
    echo this is not an aws batch job
    SRA_ACCESSION=$(sed '1q;d' accessionlist.txt)
    scratch=.
    mkdir -p ~/ncbi/dbGaP-17102
fi

cd ~/ncbi/dbGaP-17102


echo SRA_ACCESSION is $SRA_ACCESSION

echo scratch is $scratch


echo get size of $SRA_ACCESSION ...
prefetch -s $SRA_ACCESSION

echo downloading $SRA_ACCESSION from sra...
if [ -f ~/ncbi/dbGaP-17102/sra/$SRA_ACCESSION.sra ]; then
  echo SRA file already exists, skipping download
else
  if prefetch --transport http --max-size 100000000000 $SRA_ACCESSION ; then
    echo finished downloading, prefetch exited with result code 0
  else
    result=$?
    echo prefetch exited with nonzero result code $result, cleaning up and exiting...
    rm -f ~/ncbi/dbGaP-17102/sra/$SRA_ACCESSION.sra
    rm -f ~/ncbi/public/sra/* ~/ncbi/public/refseq/*
    exit $result
  fi
fi

# ( downloads to ~/ncbi/public/sra/)

viruses=( hhv6a hhv6b hhv-7 )

echo starting pipeline...

for virus in "${viruses[@]}"; do
  echo processing $virus ...
  if aws s3api head-object --bucket $BUCKET_NAME --key $PREFIX/$SRA_ACCESSION/$virus/$SRA_ACCESSION.sam.gz  &> /dev/null; then
    echo output file already exists in S3, skipping....
  else
    time (fastq-dump -Z ~/ncbi/dbGaP-17102/sra/$SRA_ACCESSION.sra |pv -i 59 -f -N "fastq-dump $virus"| \
      bowtie2 -x /bt2/$virus - | pv -i 59 -f -N "bowtie2 $virus" | \
      gzip -1 | pv -i 59 -f -N "gzip $virus" | \
      aws s3 cp - s3://$BUCKET_NAME/$PREFIX/$SRA_ACCESSION/$virus/$SRA_ACCESSION.sam.gz )
  fi
done




echo done with pipeline, cleaning up

if [[ -v AWS_BATCH_JOB_ID ]]
then
    rm -rf $scratch
fi


echo exiting...

} 2>&1 | tee /tmp/batch.log
