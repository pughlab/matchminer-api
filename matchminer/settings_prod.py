# set values from development.
from matchminer.settings_dev import *

# start url with api.
URL_PREFIX = 'api'

# enable operations log.
OPLOG = True

# enable debug.
DEBUG = True

# TOKEN TIMEOUT.
TOKEN_TIMEOUT = 60

# production data.
DATA_DIR = "/mm_staging"
DATA_CLINICAL_CSV = os.path.join(DATA_DIR, "clinical.pkl")
DATA_GENOMIC_CSV = os.path.join(DATA_DIR, "genomic.pkl")
DATA_ONCOTREE_FILE = os.getenv("ONCOTREE_CUSTOM_DIR", "/api/matchminer/data/oncotree_file.txt")

MONGO_QUERY_BLACKLIST = ['$where']

###### Copied settings from dev ######
# collection names.
COLLECTION_CLINICAL = "clinical"
COLLECTION_GENOMIC = "genomic"

# enable larger returns.
PAGINATION_LIMIT = 100000
PAGINATION_DEFAULT = 100000

# disable caching.
CACHE_CONTROL = ''
CACHE_EXPIRES = 0

# Enable reads (GET), inserts (POST) and DELETE for resources/collections
# (if you omit this line, the API will default to ['GET'] and provide
# read-only access to the endpoint).
RESOURCE_METHODS = ['GET', 'POST']
###### Copied settings from dev ######

clinical = {
    'schema': matchminer.data_model.clinical_schema,
    "allowed_read_roles": ["admin", "service", "user"],
    "allowed_write_roles": ["admin", "service"],
    'mongo_indexes': {'FIRST_LAST': [('FIRST_LAST', 1)]},
    'item_methods': ['GET', 'PATCH', 'PUT', 'DELETE']
}

trial_match = {
    'schema': matchminer.data_model.trial_match_schema,
    'allow_unknown': False,
    'allowed_read_roles': ["admin", "service", "oncologist", "cti"],
    'allowed_write_roles': ["admin", "service", "oncologist", "cti"],
    'item_methods': ['GET']
}

ctims_trial_match = {
    'schema': matchminer.data_model.ctims_trial_match_schema,
    'datasource': {
        'source': 'trial_match'
    },
    'allow_unknown': False,
    'allowed_read_roles': ["admin", "service", "oncologist", "cti"],
    'allowed_write_roles': ["admin", "service", "oncologist", "cti"],
    'item_methods': ['GET']
}
