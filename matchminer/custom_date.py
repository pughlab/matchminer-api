import json
from datetime import datetime


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            # convert datetime object to string in ISO 8601 format
            return obj.isoformat()
        return super(DateTimeEncoder, self).default(obj)
