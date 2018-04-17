# build me as fredhutch/sra-pipeline

FROM ubuntu:16.04

RUN apt-get update -y


RUN apt-get install -y  curl bzip2 perl build-essential libssl-dev unzip htop pv python3-pip

RUN  curl -LO http://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/2.9.0/sratoolkit.2.9.0-ubuntu64.tar.gz
RUN tar zxf sratoolkit.2.9.0-ubuntu64.tar.gz


ADD prj_17102.ngc /
RUN /sratoolkit.2.9.0-ubuntu64/bin/vdb-config --import prj_17102.ngc

RUN curl -LO https://github.com/BenLangmead/bowtie2/releases/download/v2.3.4.1/bowtie2-2.3.4.1-linux-x86_64.zip

RUN unzip bowtie2-2.3.4.1-linux-x86_64.zip

RUN pip3 install awscli

ADD bt2/ /bt2/

RUN chmod -R a+r /bt2

ADD run.sh /


RUN adduser --disabled-password --gecos "" neo

RUN cp /*.ngc /home/neo/ && chown neo /home/neo/*.ngc

USER neo

ENV PATH="${PATH}:/bowtie2-2.3.4.1-linux-x86_64/:/sratoolkit.2.9.0-ubuntu64/bin/"

WORKDIR /home/neo

RUN vdb-config --import /home/neo/prj_17102.ngc

RUN curl -LO https://download.asperasoft.com/download/sw/connect/3.7.4/aspera-connect-3.7.4.147727-linux-64.tar.gz
RUN tar zxf aspera-connect-3.7.4.147727-linux-64.tar.gz
RUN bash aspera-connect-3.7.4.147727-linux-64.sh


CMD /run.sh
