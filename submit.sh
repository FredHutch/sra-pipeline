#!/bin/bash

# run this in the directory where 
# https://github.com/FredHutch/nextflow-aws-batch-squared is cloned




# if [ "$TOWER_TOKEN" = "" ]
# then
#    echo "TOWER_TOKEN not set, exiting"
#    exit 1
# fi



COMMAND=$(cat <<'EOF'
./run.py \
  --job-role-arn arn:aws:iam::064561331775:role/fh-pi-jerome-k-batchtask 
  --working-directory s3://fh-pi-jerome-k/nextflow-work/ 
  --config-file s3://fh-pi-jerome-k/nextflow-scripts/nipt_pipeline/nextflow.config 
  --job-queue spot-test 
  --name nipt-pipeline-headnode-0 
  --workflow s3://fh-pi-jerome-k/nextflow-scripts/nipt_pipeline/ 
  --watch 
EOF
)

if [ "$TOWER_TOKEN" = "" ]
then
   echo "TOWER_TOKEN not set"
else
   COMMAND="${COMMAND} --tower-token $TOWER_TOKEN"
fi

eval $COMMAND

