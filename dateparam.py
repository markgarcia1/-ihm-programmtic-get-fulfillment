import datetime
import json


class DateParameter:

    def __init__(self ,key, value):
        self.key =key
        format_str = '%m/%d/%Y'
        datetime_obj = datetime.datetime.strptime(value, format_str)
        self.value =datetime_obj.date().strftime('%m/%d/%Y')
        return

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True, indent=None)
        #return json.dumps(self, default=lambda o: o.__dict__,
        #                  sort_keys=True, indent=4)