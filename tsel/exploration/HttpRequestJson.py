# Load Json into a Python object
#
import urllib2
import json
req = urllib2.Request("http://10.250.200.217:8001/?oprid=getAll&msisdn=6282293718474&show=json")
opener = urllib2.build_opener()
f = opener.open(req)
json = json.loads(f.read())
print json

