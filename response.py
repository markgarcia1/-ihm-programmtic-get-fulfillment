import json


class Response:

    def __init__(self, status_code, data, message):
        self.statusCode = status_code
        self.message = ""
        self.body = data

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)

