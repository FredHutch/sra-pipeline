#!/usr/bin/python
"""

## first argument is the filename ##
## second argument is the index name ##
"""
import sys
import os


def main():
    "do the work"
    filename = sys.argv[1]
    index = sys.argv[2]

    indexfilename = "indexes/" + index + ".fasta"
    indexbuild = "bowtie2-build " + indexfilename + " " + index
    os.system(indexbuild)

    read1 = filename + ".1.fastq.gz"
    read2 = filename + ".2.fastq.gz"
    samname = filename + ".sam"
    bowtiealign = (
        "bowtie2 --local --no-unal -p 8 "
        + "-x "
        + index
        + " -1 "
        + read1
        + " -2 "
        + read2
        + " -S "
        + samname
    )
    print(bowtiealign)
    os.system(bowtiealign)


if __name__ == "__main__":
    main()
