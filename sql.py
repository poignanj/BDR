import json
import sys


with open(sys.argv[1]) as json_file:  
    data = json.load(json_file)
    for p in data:
        print('Name: ' + p['name'])
        print('Wizard level: ' + p['levels']['sorcerer/wizard'])
        #print('compos: ' + p['components'])
        print('')