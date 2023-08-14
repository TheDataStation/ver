FROM mcr.microsoft.com/openjdk/jdk:17-ubuntu

RUN apt-get update
RUN apt-get  install -y python3
RUN apt-get install -y python3-pip
RUN apt-get install -y git
RUN alias python=python3
RUN alias pip=pip3
RUN echo 'alias python=python3' >> ~/.bashrc
RUN echo 'alias pip=pip3' >> ~/.bashrc

COPY . /ver
WORKDIR /ver

RUN    pip3 install -r requirements_quick_start.txt

WORKDIR /ver/ddprofiler
RUN bash build.sh

WORKDIR /ver
RUN python3 ver_cli.py create_sources_file quickstart
RUN python3 ver_cli.py add_csv quickstart quickstart demo_dataset/
RUN python3 ver_cli.py profile quickstart output_profiles_json
RUN python3 ver_cli.py build_dindex output_profiles_json/ --force

CMD ["bash"]
