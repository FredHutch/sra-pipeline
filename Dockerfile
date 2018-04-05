# build me as fredhutch/sra-pipeline

FROM ubuntu:16.04

RUN apt-get update -y
RUN apt-get update -y


# TODO install awscli

RUN apt-get install -y  curl bzip2 perl build-essential libssl-dev unzip


# RUN curl -O https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh

RUN curl -O https://repo.continuum.io/miniconda/Miniconda3-4.3.31-Linux-x86_64.sh

# RUN chmod +x Miniconda3-latest-Linux-x86_64.sh

RUN chmod +x Miniconda3-4.3.31-Linux-x86_64.sh

# RUN ./Miniconda3-latest-Linux-x86_64.sh -b

RUN ./Miniconda3-4.3.31-Linux-x86_64.sh -b

RUN  /root/miniconda3/bin/conda config --add channels conda-forge
RUN  /root/miniconda3/bin/conda config --add channels bioconda

# RUN /root/miniconda3/bin/conda update -y -n base conda


RUN /root/miniconda3/bin/conda install -y parallel-fastq-dump

RUN  curl -LO http://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/2.9.0/sratoolkit.2.9.0-ubuntu64.tar.gz
RUN tar zxf sratoolkit.2.9.0-ubuntu64.tar.gz


# TODO in production, get ngc file from elsewhere, do NOT store it in github, or
# in docker image if that's possible.
RUN  curl -O ftp://ftp.ncbi.nlm.nih.gov/sra/examples/decrypt_examples/prj_phs710EA_test.ngc
RUN /sratoolkit.2.9.0-ubuntu64/bin/vdb-config --import prj_phs710EA_test.ngc

RUN curl -LO https://github.com/BenLangmead/bowtie2/releases/download/v2.3.4.1/bowtie2-2.3.4.1-linux-x86_64.zip

RUN unzip bowtie2-2.3.4.1-linux-x86_64.zip

ENV PATH="${PATH}:/root/miniconda3/bin/:/bowtie2-2.3.4.1-linux-x86_64/:/sratoolkit.2.9.0-ubuntu64/bin/"

RUN pip install awscli

ADD bt2/ /bt2/

ADD run.sh /

CMD /run.sh
