import json

class Request:

    def __init__(self, event):
        self.headers = ""
        self.params = event['queryStringParams']

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__,
                          sort_keys=True, indent=4)