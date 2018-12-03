workflow sra_pipeline {
    File ngcFile
    File listOfAccessions
    File virusesFile

    Array[File] arrayOfAccessions = read_lines(listOfAccessions)

    scatter (accession in arrayOfAccessions) {
        call process_accession {
            input:
                accession = accession,
                ngcFile = ngcFile, 
                virusesFile = virusesFile
        }

        output {
            Array[File] samDir = process_accession.samDir
        }
    }





}


task process_accession {
    String accession
    File ngcFile
    String ngc = sub(basename(ngcFile, ".ngc"), "prj_", "")
    File virusesFile

    command {
        set -e
        rm -rf ~/ncbi
        ln -s $(pwd) ~/ncbi
        /sratoolkit.2.9.2-ubuntu64/bin/vdb-config --import ${ngcFile}
        pushd $HOME/ncbi/dbGaP-${ngc}
        mkdir -p sra
        prefetch --transport http --max-size 100000000000 ${accession}
        mkdir -p ptmp
        # TODO unhardcode --threads value (and path to executable)
        /root/miniconda3/bin/parallel-fastq-dump --sra-id sra/${accession}.sra --threads 8 \
          --gzip --split-files -W -I --tmpdir ptmp
        # TODO build bt2 files on the fly, download fastas from s3
        for virus in $(cat ${virusesFile}); do
            # process "$virus"
            # TODO unhardcode number of cores (value of -p below)
            set +e
            # TODO handle bowtie2 output to sam file???
            mkdir sam_output
            bowtie2 --local -p 8 --no-unal -x /bt2/$virus \
              -1 ${accession}_1.fastq.gz \
              -2 ${accession}_2.fastq.gz \
              > sam_output/${accession}_$virus.sam \
              2> bowtie2.stderr
            rc=$?
            set -e
            if [[ $rc != 134 ]]
            then 
                if grep -q "fewer reads in file specified with -2" bowtie2.stderr
                then
                    bowtie2 --local -p 8 --no-unal -x /bt2/$virus \
                      -U ${accession}_1.fastq.gz \
                      > sam_output/${accession}_$virus.sam \

                    
                elif grep -q "fewer reads in file specified with -1" bowtie2.stderr
                then
                    bowtie2 --local -p 8 --no-unal -x /bt2/$virus \
                      -U ${accession}_2.fastq.gz \
                      > sam_output/${accession}_$virus.sam \

                fi
            fi
        done        

    }

    runtime {
        docker: "fredhutch/sra-pipeline:root-only.1"
        cpu: 8
        mem: "10 GB"
    }

    output {
        File samDir = "sam_output/"
    }

}