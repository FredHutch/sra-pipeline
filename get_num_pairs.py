#!/usr/bin/env python3

"""
Get the number of fastq pairs.
Needed for size of array job.
"""

from collections import defaultdict

import boto3

# 2300 pairs total?


def get_pairs():
    "do the work"
    s3 = boto3.client("s3")  # pylint: disable=invalid-name

    args = dict(
        Bucket="fh-pi-jerome-k", Delimiter="/", Prefix="nipt_pipeline/all_fastqs/"
    )

    keys = []

    while True:
        resp = s3.list_objects_v2(**args)
        keys.extend([x["Key"] for x in resp["Contents"]])
        if not resp["IsTruncated"]:
            break
        args["ContinuationToken"] = resp["NextContinuationToken"]

    keydict = defaultdict(int)

    for key in keys:
        if not key.endswith(".fastq.gz"):
            continue
        key = key.replace(".1.fastq.gz", "")
        key = key.replace(".2.fastq.gz", "")
        keydict[key] += 1

    return [k for k, v in keydict.items() if v == 2]


if __name__ == "__main__":
    print(len(get_pairs()))
