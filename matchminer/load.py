from datetime import datetime
import json
import logging
import os
from argparse import Namespace

import pymongo
import yaml
from bson import json_util


logging.basicConfig(level=logging.INFO)
log = logging.getLogger('matchengine')


class LoadTrialInput():
    def __init__(self, trial_format, trial_input, trial_fp=None):
        self.trial_format = trial_format
        self.trial_input = trial_input
        self.trial = trial_fp


def get_mongo_client():
    host = 'localhost'
    port = 27017
    client = pymongo.MongoClient(host, port)
    return client.matchminer


def load(load_input: LoadTrialInput):
    """
    Load data into MongoDB for matching.

    Depending on the naming conventions used in your data, it may be necessary to either alter the data itself,
    or to define custom transformation functions in order for matching to work correctly.

    These transformations to data to prepare for matching can be made using the config.json file,
    and custom functions as described in match_criteria_transform.json.

    For more information and examples, see the README.
    """

    my_client = get_mongo_client()
    log.info(f"Database: matchminer")
    if load_input.trial or load_input.trial_input:
        log.info('Adding trial(s) to mongo...')
        load_trials(my_client, load_input)

    log.info('Done.')


#################
# trial loading
#################
def load_trials(db_rw, args: LoadTrialInput):
    if args.trial_format == 'json':
        # load_trials_json(args, db_rw)
        load_from_memory(db_rw, args.trial_input, 'json')
    elif args.trial_format == 'yml':
        load_trials_yaml(args, db_rw)


def load_trials_yaml(args: Namespace, db_rw):
    if os.path.isdir(args.trial):
        load_dir(args, db_rw, "yml", args.trial, 'trial')
    else:
        load_file(db_rw, 'yml', args.trial, 'trial')


def load_trials_json(args: Namespace, db_rw):
    if hasattr(args, "trial_input"):
        load_from_memory(args, db_rw, "json", args.trial_input, 'trial')

    # load a directory of json files
    elif os.path.isdir(args.trial):
        load_dir(args, db_rw, "json", args.trial, 'trial')
    else:
        # path leads to a single JSON file
        if is_valid_single_json(args.trial):
            load_file(db_rw, 'json', args.trial, 'trial')

        else:
            with open(args.trial) as file:
                json_raw = file.read()
                success = None
                try:
                    # mongoexport by default exports each object on a new line
                    json_array = json_raw.split('\n')
                    for doc in json_array:
                        data = json.loads(doc)
                        db_rw.trial.insert_one(data)
                    success = True
                except json.decoder.JSONDecodeError as e:
                    log.debug(f"{e}")
                if not success:
                    try:
                        # mongoexport also allows an export as a json array
                        json_array = json.loads(json_raw)
                        for doc in json_array:
                            db_rw.trial.insert_one(doc)
                        success = True
                    except json.decoder.JSONDecodeError as e:
                        log.debug(f"{e}")
                        if not success:
                            log.warning(
                                'Cannot read json format. JSON documents must be either newline separated, '
                                'in an array, or loaded as separate documents ')
                            raise Exception("Unknown JSON Format")


##################
# util functions
##################
def load_from_memory(db_rw, json_list: list[dict], filetype: str):
    for data in json_list:
        if is_valid_single_json_dict(data):
            for key in list(data.keys()):
                if key == 'BIRTH_DATE':
                    data[key] = convert_birthdate(data[key])
                    data['BIRTH_DATE_INT'] = int(data[key].strftime('%Y%m%d'))
            db_rw.trial.insert_one(data)


def load_dir(args: Namespace, db_rw, filetype: str, path: str, collection: str):
    for filename in os.listdir(path):
        if filename.endswith(f".{filetype}"):
            val = vars(args)[collection]
            full_path = val + filename if val[-1] == '/' else val + '/' + filename
            load_file(db_rw, filetype, full_path, collection)


def load_file(db_rw, filetype: str, path: str, collection: str):
    with open(path) as file_handle:
        raw_file_data = file_handle.read()
        if filetype == 'yml':
            data = yaml.safe_load_all(raw_file_data)
            db_rw[collection].insert_many(data)
        elif filetype == 'json':
            if is_valid_single_json(path):
                data = json_util.loads(raw_file_data)
                for key in list(data.keys()):
                    if key == 'BIRTH_DATE':
                        data[key] = convert_birthdate(data[key])
                        data['BIRTH_DATE_INT'] = int(data[key].strftime('%Y%m%d'))
                db_rw[collection].insert_one(data)


def convert_birthdate(birth_date):
    """Convert a string birthday to to datetime object"""
    try:
        birth_date_dt = datetime.strptime(birth_date, "%Y-%m-%d")
    except Exception as e:
        log.warn("Unable to import clinical data due to malformed "
                 "patient birth date. \n\nBirthdates must be strings with "
                 "the following format \n %Y-%m-%d \n 2019-10-27 ")
        raise ImportError
    return birth_date_dt

def is_valid_single_json(path: str):
    """Check if a JSON file is a single object or an array of JSON objects"""
    try:
        with open(path) as f:
            json_file = json.load(f)
            if json_file.__class__ is list:
                return False
            return True
    except (FileNotFoundError, json.decoder.JSONDecodeError) as e:
        if e.__class__ is FileNotFoundError:
            log.error(f"{e}")
            raise e
        elif e.__class__ is json.decoder.JSONDecodeError:
            return False


def is_valid_single_json_dict(json_dict: dict):
    """Check if a JSON file is a single object or an array of JSON objects"""
    if json_dict.__class__ is list:
        return False
    return True
