
FROM ubuntu:16.04

RUN apt-get update -y


RUN apt-get update -y && apt-get install -y  curl bzip2 perl build-essential libssl-dev unzip htop pv software-properties-common python-software-properties

RUN add-apt-repository ppa:jonathonf/python-3.6 -y

RUN apt-get update -y

RUN apt-get install -y python3.6

RUN curl https://bootstrap.pypa.io/get-pip.py -o /tmp/get-pip.py

RUN python3.6 /tmp/get-pip.py

RUN  curl -LO http://ftp-trace.ncbi.nlm.nih.gov/sra/sdk/2.9.0/sratoolkit.2.9.0-ubuntu64.tar.gz
RUN tar zxf sratoolkit.2.9.0-ubuntu64.tar.gz

RUN curl -LO https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh

ADD prj_17102.ngc /
RUN /sratoolkit.2.9.0-ubuntu64/bin/vdb-config --import prj_17102.ngc

RUN curl -LO https://github.com/BenLangmead/bowtie2/releases/download/v2.3.4.1/bowtie2-2.3.4.1-linux-x86_64.zip

RUN unzip bowtie2-2.3.4.1-linux-x86_64.zip

RUN pip3.6 install awscli requests sh

ADD bt2/ /bt2/

RUN chmod -R a+r /bt2

ADD run.py /


RUN adduser --disabled-password --gecos "" neo

RUN cp /*.ngc /home/neo/ && chown neo /home/neo/*.ngc

RUN curl -L https://raw.githubusercontent.com/FredHutch/url-fetch-and-run/master/fetch-and-run/fetch_and_run.sh > /usr/local/bin/fetch_and_run.sh

RUN chmod a+x /usr/local/bin/fetch_and_run.sh

USER neo

ENV PATH="${PATH}:/bowtie2-2.3.4.1-linux-x86_64/:/sratoolkit.2.9.0-ubuntu64/bin/"

WORKDIR /home/neo

RUN curl -LO https://repo.continuum.io/miniconda/Miniconda3-latest-Linux-x86_64.sh

RUN bash Miniconda3-latest-Linux-x86_64.sh -b -p /home/neo/miniconda3

ENV PATH="${PATH}:/home/neo/miniconda3/bin/"

RUN conda config --add channels defaults &&  conda config --add channels conda-forge &&conda config --add channels bioconda

RUN conda install -y parallel-fastq-dump

RUN vdb-config --import /home/neo/prj_17102.ngc

RUN curl -LO https://download.asperasoft.com/download/sw/connect/3.7.4/aspera-connect-3.7.4.147727-linux-64.tar.gz
RUN tar zxf aspera-connect-3.7.4.147727-linux-64.tar.gz
RUN bash aspera-connect-3.7.4.147727-linux-64.sh



ENTRYPOINT ["/usr/local/bin/fetch_and_run.sh"]
