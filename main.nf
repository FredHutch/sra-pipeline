#!/usr/bin/env nextflow
echo true

cheers = Channel.from 'Bonjour', 'Ciao', 'Hello', 'Hola'

process sayHello0 {
  // container "ubuntu:latest"
  container "comics/bowtie2"
  cpus 8
  memory '10 GB'
  input:
    val x from cheers
  script:
    """
    echo '$x world!'
    """
}