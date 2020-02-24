import json

class Parameter:

   def __init__(self,key, value):
         self.key=key
         self.value=value
         return

   def toJSON(self):
       return json.dumps(self,default=lambda o: o.__dict__, sort_keys=True, indent=None)
       # return json.dumps(self, default=lambda o: o.__dict__,
       #                   sort_keys=True, indent=4)