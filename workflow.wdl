workflow sra_pipeline {
    File ngcFile
    File listOfAccessions
    String listOfViruses

    Array[File] arrayOfAccessions = read_lines(listOfAccessions)

    scatter (accession in arrayOfAccessions) {
        call process_accession {
            input:
                accession = accession,
                ngcFile = ngcFile, 
                listOfViruses = listOfViruses
                
        }
    }


  output {
    Array[String] out = process_accession.out
    Array[String] err = process_accession.err
    # TODO ....
    # Array[File] outputPizzlyJson = Pizzly.output_json
    # Array[File] outputPizzlyFasta = Pizzly.output_fasta
    # # do we want to keep intermediate (Picard & Kallisto) outputs?
    # # let's say yes for now...
    # Array[File] outputKallisto = Kallisto.output_fusion
    # Array[File] outputPicard1 = Picard.output_fastq1
    # Array[File] outputPicard2 = Picard.output_fastq2
  }



}


task process_accession {
    String accession
    File ngcFile
    String ngc = sub(basename(ngcFile, ".ngc"), "prj_", "")
    String listOfViruses

    command {
        set -e
        rm -rf ~/ncbi
        ln -s $(pwd) ~/ncbi
        /sratoolkit.2.9.2-ubuntu64/bin/vdb-config --import ${ngcFile}
        pushd $HOME/ncbi/dbGaP-${ngc}
        mkdir -p sra
        prefetch --transport http --max-size 100000000000 ${accession}
        mkdir -p ptmp
        # TODO unhardcode --threads value
        /home/neo/miniconda3/bin/parallel-fastq-dump --sra-id sra/${accession}.sra --threads 8 \
          --gzip --split-files -W -I --tmpdir ptmp
        # TODO build bt2 files on the fly, download fastas from s3
        IFS=',' read -ra ADDR <<< "${listOfViruses}"
        for virus in "${ADDR[@]}"; do
            # process "$virus"
            # TODO unhardcode number of cores (value of -p below)
            set +e
            # TODO handle bowtie2 output to sam file???
            bowtie2 --local -p 8 --no-unal -x /bt2/$virus \
              -1 ${accession}_1.fastq.gz \
              -2 ${accession}_2.fastq.gz \
              2> bowtie2.stderr
            rc=$?
            set -e
            if [[ $rc != 134 ]]
            then 
                if grep -q "fewer reads in file specified with -2" bowtie2.stderr
                then
                    bowtie2 --local -p 8 --no-unal -x /bt2/$virus \
                      -U ${accession}_1.fastq.gz 
                    
                elif grep -q "fewer reads in file specified with -1" bowtie2.stderr
                then
                    bowtie2 --local -p 8 --no-unal -x /bt2/$virus \
                      -U ${accession}_2.fastq.gz 
                fi
                done
            fi
            done
        done        

    }

    runtime {
        docker: "fredhutch/sra-pipeline-no-entrypoint:1"
        cpu: 8
        mem: "10 GB"
    }

    output {
        String out = read_string(stdout())
        String err = read_string(stderr())
        # TODO ....
    }

}