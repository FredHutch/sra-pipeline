#!/usr/bin/env python3

"""
Get the number of fastq pairs.
Needed for size of array job.
"""

from collections import defaultdict

import boto3

# 2300 pairs total?


def list_bucket(bucket, prefix):
    "list bucket/prefix w/pagination"
    rsrc = boto3.resource("s3")
    bkt = rsrc.Bucket(bucket)
    keys = [x for x in bkt.objects.filter(Prefix=prefix)]
    return [x.key for x in keys]

def get_all_pairs():
    "do the work"
    keys = list_bucket("fh-pi-jerome-k", "nipt_pipeline/all_fastqs") # TODO unhardcode
    keydict = defaultdict(int)


    for key in keys:
        if not key.endswith(".fastq.gz"):
            continue
        key = key.replace(".1.fastq.gz", "")
        key = key.replace(".2.fastq.gz", "")
        keydict[key] += 1

    return [k for k, v in keydict.items() if v == 2]

def get_unfinished_pairs():
    "get unfinished pairs"
    all_pairs = [x.split("/")[-1] for x in get_all_pairs()]
    output_keys = list_bucket("fh-pi-jerome-k", "nipt_pipeline/output") # TODO unhardcode
    keydict = defaultdict(int)
    for output_key in output_keys:
        key = output_key.split("/")[-1].replace(".sam", "")
        keydict[key] += 1
    num_refs = 3 # TODO unhardcode
    finished = [k for k, v in keydict.items() if v == num_refs]
    allp = set(all_pairs)
    unfinished = allp - set(finished)
    return list(unfinished)


if __name__ == "__main__":
    print(len(get_all_pairs()))
