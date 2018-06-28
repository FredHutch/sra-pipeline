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
container_id=$(cat /proc/self/cgroup | head -n 1 | cut -d '/' -f4)
echo container id is $container_id

aws s3 cp $ACCESSION_LIST accessionlist.txt

aws configure set default.s3.multipart_chunksize 50MB
aws configure set default.s3.max_concurrent_requests 100
aws configure set default.s3.max_queue_size 10000
aws configure set default.s3.multipart_threshold 64MB


if [ -z $NUM_CORES ]; then
  echo NUM_CORES is not set, exiting
  exit 1
else
  echo NUM_CORES is set to $NUM_CORES
fi

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
PTMP=tmp
mkdir -p $PTMP
rm -rf $PTMP/*

echo SRA_ACCESSION is $SRA_ACCESSION

echo scratch is $scratch

# echo get size of $SRA_ACCESSION ...
# prefetch -s $SRA_ACCESSION

# interval=$(RANDOM=$$ shuf -i 0-60 -n 1)
# echo sleeping $interval minutes before download to avoid slamming SRA....

# sleep ${interval}m

# echo downloading $SRA_ACCESSION from sra...
# if [ -f ~/ncbi/dbGaP-17102/sra/$SRA_ACCESSION.sra ]; then
#   echo SRA file already exists, skipping download
# else
#   if prefetch --transport http --max-size 100000000000 $SRA_ACCESSION ; then
#     echo finished downloading, prefetch exited with result code 0
#   else
#     result=$?
#     echo prefetch exited with nonzero result code $result, cleaning up and exiting...
#     rm -f ~/ncbi/dbGaP-17102/sra/$SRA_ACCESSION.sra
#     rm -f ~/ncbi/public/sra/* ~/ncbi/public/refseq/*
#     exit $result
#   fi
# fi

fastq_url=s3://$BUCKET_NAME/pipeline-fastq-salivary/$SRA_ACCESSION/$SRA_ACCESSION.fastq.gz

# echo streaming fastq-dump output to s3...
#
# time (fastq-dump -Z ~/ncbi/dbGaP-17102/sra/$SRA_ACCESSION.sra | pv -i 59 -N fastq-dump |gzip| pv -i 59 -N gzip | aws s3 cp - $fastq_url)

# ( downloads to ~/ncbi/public/sra/)

# echo running fastq-dump
# time parallel-fastq-dump --sra-id sra/$SRA_ACCESSION.sra --threads $NUM_CORES --outdir . --gzip --split-files -W -I --tmpdir $PTMP

# echo "done with fastq-dump, copying fastqs to s3"

# aws s3 cp ${SRA_ACCESSION}_1.fastq.gz s3://$BUCKET_NAME/pipeline-fastq-salivary/$SRA_ACCESSION/
# aws s3 cp ${SRA_ACCESSION}_2.fastq.gz s3://$BUCKET_NAME/pipeline-fastq-salivary/$SRA_ACCESSION/



# viruses=( hhv6a hhv6b hhv-7 gapdhpolyAtrimmed )
viruses=( hhv6a_u1102_untrimmed hhv6b_z29_untrimmed hhv-7 gapdhpolyAtrimmed )


echo starting pipeline...


echo getting fastqs from s3...

# aws s3 cp s3://$BUCKET_NAME/pipeline-fastq/$SRA_ACCESSION/${SRA_ACCESSION}_1.fastq.gz .
# aws s3 cp s3://$BUCKET_NAME/pipeline-fastq/$SRA_ACCESSION/${SRA_ACCESSION}_2.fastq.gz .

set +e

aws s3 cp s3://$BUCKET_NAME/pipeline-fastq/$SRA_ACCESSION/${SRA_ACCESSION}_1.fastq.gz .
aws s3 cp s3://$BUCKET_NAME/pipeline-fastq/$SRA_ACCESSION/${SRA_ACCESSION}_2.fastq.gz .

aws s3 cp s3://$BUCKET_NAME/pipeline-fastq-salivary/$SRA_ACCESSION/${SRA_ACCESSION}_1.fastq.gz .
aws s3 cp s3://$BUCKET_NAME/pipeline-fastq-salivary/$SRA_ACCESSION/${SRA_ACCESSION}_2.fastq.gz .

set -e

for virus in "${viruses[@]}"; do
# virus="betaglobincds"

  echo processing $virus ...
  if aws s3api head-object --bucket $BUCKET_NAME --key $PREFIX/$SRA_ACCESSION/$virus/$SRA_ACCESSION.sam  &> /dev/null; then
    echo output file already exists in S3, skipping....
  else
    time bowtie2 --local -p $NUM_CORES --no-unal -1 ${SRA_ACCESSION}_1.fastq.gz -2 ${SRA_ACCESSION}_2.fastq.gz -x /bt2/$virus | \
      pv -i 31 -f -N "bowtie2 $virus" | \
      aws s3 cp - s3://$BUCKET_NAME/$PREFIX/$SRA_ACCESSION/$virus/$SRA_ACCESSION.sam
  fi
done




echo done with pipeline, cleaning up

# echo removing fastq file from s3...
# aws s3 rm $fastq_url

echo removing scratch...

if [[ -v AWS_BATCH_JOB_ID ]]
then
    rm -rf $scratch
fi



echo exiting...

} 2>&1 | tee /tmp/batch.log
