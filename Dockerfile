FROM python:3.8-slim-buster

# Install packages needed to compile dependencies:
RUN DEBIAN_FRONTEND=noninteractive apt-get update --fix-missing && apt-get install -y \
    libxml2-dev \
    libxslt1-dev \
    libxmlsec1-dev \
    libxmlsec1-openssl \
    pkg-config \
    git \
    gcc

# Configure pip
# Counterintuitively, PIP_NO_COMPILE=no is needed to turn compiling *off*
# We do this partly because compiling is not reproducible, creating pointless diffs in images,
# and partly to reduce image size.
ENV PIP_NO_CACHE_DIR=1 PIP_NO_COMPILE=no


RUN mkdir /matchminerAPI

# Install requirements:
COPY requirements.txt /matchminerAPI/requirements.txt
WORKDIR /matchminerAPI
RUN pip install -r requirements.txt

# Hack to work around https://github.com/py-bson/bson/issues/82
RUN pip --no-input uninstall --yes bson
RUN pip --no-input uninstall --yes pymongo
RUN pip install 'pymongo==3.10'

ENV ONCOTREE_CUSTOM_DIR /matchminerAPI/data/oncotree_file.txt

# Use an (anonymous) volume to hold "*.pyc" files:
VOLUME /bytecode
RUN mkdir -p /bytecode
ENV PYTHONPYCACHEPREFIX=/bytecode

# Expose and bind to port 80:
#EXPOSE 80
#CMD gunicorn wsgi:app --bind=0.0.0.0:80

# Copy over source files:
COPY . /matchminerAPI/

WORKDIR /matchminerAPI/matchengineV2
RUN pip install .

RUN pip --no-input uninstall --yes bson
RUN pip --no-input uninstall --yes pymongo
RUN pip install 'pymongo==3.10'

WORKDIR /matchminerAPI

ARG MM_API_TOKEN
ENV MM_API_TOKEN=$MM_API_TOKEN

ENV SECRETS_JSON=/matchminerAPI/secrets_json_prod.json
ENV MM_SETTINGS=PROD
ENV NO_AUTH=False

CMD python pymm_run.py serve
