# build me as fredhutch/sra-pipeline

FROM ubuntu:16.04

# be sure to change this if you change versions of bowtie2 or sratoolkit that you download
ENV PATH="${PATH}:/root/miniconda3/bin/:/bowtie2-2.3.4.1-linux-x86_64/:/sratoolkit.2.9.0-ubuntu64/bin/"


RUN apt-get update -y
RUN apt-get update -y


RUN apt-get install -y  curl bzip2 perl build-essential libssl-dev unzip htop



RUN curl -O https://repo.continuum.io/miniconda/Miniconda3-4.3.31-Linux-x86_64.sh


RUN chmod +x Miniconda3-4.3.31-Linux-x86_64.sh


RUN ./Miniconda3-4.3.31-Linux-x86_64.sh -b

RUN  conda config --add channels conda-forge
RUN  conda config --add channels bioconda



RUN conda install -y parallel-fastq-dump

RUN  curl -LO http://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/2.9.0/sratoolkit.2.9.0-ubuntu64.tar.gz
RUN tar zxf sratoolkit.2.9.0-ubuntu64.tar.gz


ADD prj_17102.ngc /
RUN vdb-config --import prj_17102.ngc

RUN curl -LO https://github.com/BenLangmead/bowtie2/releases/download/v2.3.4.1/bowtie2-2.3.4.1-linux-x86_64.zip

RUN unzip bowtie2-2.3.4.1-linux-x86_64.zip


RUN pip install awscli

ADD bt2/ /bt2/

RUN chmod -R a+r /bt2

ADD run.sh /


RUN adduser --disabled-password --gecos "" neo

RUN chmod a+rwx /scratch

USER neo

RUN vdb-config --import /prj_17102.ngc

RUN curl -LO https://download.asperasoft.com/download/sw/connect/3.7.4/aspera-connect-3.7.4.147727-linux-64.tar.gz
RUN tar zxf aspera-connect-3.7.4.147727-linux-64.tar.gz
RUN bash aspera-connect-3.7.4.147727-linux-64.sh


CMD /run.sh
