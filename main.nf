#!/usr/bin/env nextflow
echo true // this is deprecated


filePairChannel = Channel.fromFilePairs("${params.inputDir}*.{1,2}.fastq.gz")
viralGenomeChannel = Channel.from(params.viruses)
    .map{it -> file("${params.refPath}/${it}.fasta")}

process runBowtie {
  publishDir "${params.outDir}/${virus}/"
  // container "ubuntu:latest"
  container "comics/bowtie2"
  cpus 4 // 8
  memory '2 GB' // 10
  input:
    set filename, file(reads)  from filePairChannel
    each file(genome) from viralGenomeChannel
  output:
    set val(filename), val(genome), file("*.sam")
  script:
    """
    echo "filename is $filename"
    echo "genome is $genome"
    bowtie2-build $genome ref
    bowtie2 --local --no-unal -p 4 -x ref -1 ${reads[0]} -2  ${reads[1]} -S ${filename}.sam
    """
}