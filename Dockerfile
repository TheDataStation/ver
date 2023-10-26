FROM mcr.microsoft.com/openjdk/jdk:17-ubuntu
ARG DATASET=demo_dataset/

# Install Python 3
RUN apt-get update
RUN apt-get  install -y python3
RUN apt-get install -y python3-pip
RUN apt-get install -y git

# Install Create Python alias for python3
RUN echo 'alias python=python3' >> ~/.bashrc
RUN echo 'alias pip=pip3' >> ~/.bashrc

# Copy the directory into the image
COPY . /ver
WORKDIR /ver

# Install the requirements
RUN    pip3 install -r requirements_quick_start.txt

# Build
WORKDIR /ver/ddprofiler
RUN bash build.sh

# Run ver on demo dataset
WORKDIR /ver
RUN python3 ver_cli.py create_sources_file quickstart
RUN python3 ver_cli.py add_csv quickstart quickstart $DATASET
RUN python3 ver_cli.py profile quickstart output_profiles_json
RUN python3 ver_cli.py build_dindex output_profiles_json/ --force
RUN python3 ver_quickstart.py
CMD ["bash"]
