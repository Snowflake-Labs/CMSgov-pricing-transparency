#
# This is the base image for the docker, which has miniconda installed.
# You could use the 'anaconda' base image too; however the docker image 
# size will be large and hence loading will take some larger time.
#
# There are 2 versions of the FROM statement.
# - The '--platform=linux/amd64' allows us to work locally, if the laptop is MAC-M1. 
#       However, this version has issues when running a streamlit app either in the
#       local(docker) env and also in the codespace env
#
# - The one without the 'platform' allows to run streamlit in the codespace env, 
#       However, this means that streamlit app cannot be developed via the local(docker)
#       environment. 

#FROM --platform=linux/amd64 continuumio/miniconda3
FROM continuumio/miniconda3

# We would need to download datafiles from internet, during development. Hence
# at the least install CURL into the image. Another reason is that we will be using
# this to download the SnowSQL too.
USER root
RUN apt-get update && \
    apt-get install -y curl && \
    echo "running in codespace" > /tmp/.is_codespace_env.txt # Used by execution script to verify if we are running in a codespace docker environment 

# Install SNOWSQL into the base conda environment, I tried installing SNOWSQL on the
# same env as SnowPark, unfortunately it has some complication. Hence SnowSQL
# Setting up default Python environment to support pyspark and snowflake python connector
SHELL ["conda", "run", "/bin/bash", "-c"]
RUN pip install --upgrade pip
RUN pip install --upgrade \
    snowflake-connector-python[pandas] \
    snowflake-sqlalchemy

#Setting up Snowpark Python conda environment
COPY conda_pyspark_environment.yml* /tmp/installation-scripts/ 
RUN conda create -n pysnowpark -c https://repo.anaconda.com/pkgs/snowflake python=3.8
SHELL ["conda", "run", "-n", "pysnowpark", "/bin/bash", "-c"]
RUN /opt/conda/bin/conda env update -f /tmp/installation-scripts//conda_pyspark_environment.yml

#Deploying Snowflake Connectors and Drivers
COPY install_snowflake.sh* /tmp/installation-scripts/
USER root
SHELL ["/bin/bash", "-c"]
RUN chmod +x /tmp/installation-scripts/install_snowflake.sh \
    && /tmp/installation-scripts/install_snowflake.sh \
    && rm -rf /tmp/installation-scripts/*


# Used by execution script to verify if we are running in a codespace docker environment 
RUN echo "running in codespace" > /tmp/.is_codespace_env.txt 

# RUN if [ -f "/tmp/conda-tmp/environment.yml" ]; then umask 0002 && /opt/conda/bin/conda env update -n base -f /tmp/conda-tmp/environment.yml; fi \
#     && rm -rf /tmp/conda-tmp