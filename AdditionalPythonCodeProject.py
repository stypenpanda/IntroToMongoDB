import json
import re
import pymongo
import codecs
from collections import defaultdict
import lxml.etree as etree

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]
lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
endsWithZeiten = re.compile(r'}$zeiten')

  
def shape_element(element):
    node = {}
    if element.tag == "node" or element.tag == "way" :
        #-1. Add type of node        
        node['type'] = element.tag
        #0. Add longitude and latidude if available
        if ('lat' in element.attrib) and ('lon' in element. ):
            node['pos'] = [float(element.attrib['lat']), float(element.attrib['lon'])]
        for att in element.attrib:
            #1. Check for "created" and enter
            if att in CREATED:
                if 'created' not in node:
                    node['created'] = {}
                node['created'][att] = element.attrib[att]
            #2. Add all other attributes to the dictionary
            elif att not in ['lon','lat', 'type']:
                node[att] = element.attrib[att]
            elif att == 'type':
                node['osm_type'] = element.attrib[att]
        #3. Go through all sub tags
        for tag in element.iter("tag"):
            if 'k' in tag.attrib:
                if not re.match(problemchars, tag.attrib['k']):
                    if tag.attrib['k'][:5] == 'addr:' and tag.attrib['k'][5:].find(':') == -1:
                        if 'address' not in node:
                            node['address'] = {}
                        node['address'][tag.attrib['k'][5:]] = tag.attrib['v']
                    elif tag.attrib['k'] == 'type':
                        node['osm_type'] = tag.attrib['v']
                    else:
                        node[tag.attrib['k']] = tag.attrib['v']                
        #4. For ways add node references
        if element.tag == "way" :
            for tag in element.iter("nd"):
                if 'ref' in tag.attrib:
                    if 'node_refs' not in node:
                        node['node_refs'] = []
                    node['node_refs'].append(tag.attrib['ref'])
        return node
    else:
        return None
        
def process_map(file_in = 'MYK.osm', pretty = False):
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in etree.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
                el.clear()
                #for ancestor in el.xpath('ancestor-or-self::*'):
                #    while ancestor.getprevious() is not None:
                #        del ancestor.getparent()[0]
    #pprint.pprint(data)
    pass

process_map()

lstDictMYK = []
for line in open('MYK.osm.json', 'r'):
    lstDictMYK.append(json.loads(line))
    
## Basic stats:
len(lstDictMYK)
# result: 593561
    
countNode, countWay = 0,0
for el in lstDictMYK:
    if el['type'] == 'node':
        countNode +=1
    elif el['type'] == 'way':
        countWay += 1
#result:
#countNode = 529819
#countWay = 63669



#Update contacts
contact_list = {'contact:email': 'email', 'contact:fax': 'fax', 'contact:google_plus': 'google_plus', 'contact:mobile': 'mobile', 'contact:office': 'office', 'contact:phone': 'phone', 'contact:website': 'website', 'email': 'email', 'fax': 'fax', 'phone': 'phone'}
def updateContacts(lstOfDicts, lstContact):
    newLstOfDicts = []
    counterUpdates = 0
    #1. loop through all elements in the list
    for el in lstOfDicts:
        CurrEl = el
        lstDelCurrEl = []
        contactAdded = False
        #2. loop through all keys in the dictionary
        for key in el:
            #3. if the key is in the list contact
            if key in lstContact:
                if not(contactAdded):
                    counterUpdates +=1
                    contactAdded = True
                    #4. if contact is not already a key in the dictionary, create the key with an empty dictionary
                    contact = {}
                #5. create the entry in the dict 'contact' and delete the old entry
                contact[lstContact[key]] = CurrEl[key]
                lstDelCurrEl.append(key)
        #6. delete all original keys, append to new lst of dicts and add contacts if included
        for k in lstDelCurrEl:
            del CurrEl[k]
        if contactAdded:
            CurrEl['contact'] = contact
        newLstOfDicts.append(CurrEl)
    return newLstOfDicts, counterUpdates


#Replace duplicate tags
tag_replacement = {'acc': 'access', 'alternative_name': 'alt_name', 'FIXME': 'fixme', 'Öffnungszeiten': 'opening_hours', 'Öffungszeiten': 'opening_hours', 'step.condition': 'step:condition', 'surface.material': 'surface:material'} 
def replaceDuplicateTags(lstOfDicts, lstTagMapping):
    #1. loop through all elements in the list
    newLstOfDicts = []
    counterUpdates = 0
    for el in lstOfDicts:
        CurrEl = el
        lstDelCurrEl = []
        #2. loop through all keys in the dictionary
        keys = el.keys()
        for key in keys:
            #3. if the key is in the list lstTagMapping
            if key in lstTagMapping:
                #4. create the entry with the new name and delete the old one
                counterUpdates +=1
                CurrEl[lstTagMapping[key]] = el[key]
                lstDelCurrEl.append(key)
        #5. delete all original keys and append to new dict
        for k in lstDelCurrEl:
            del CurrEl[k]
        newLstOfDicts.append(CurrEl)
    return newLstOfDicts, counterUpdates

#to be changed when reading in, otherwise not possible

"""
Nice way of investigating tags:
counter = 0
for el in lstDictMYK:
    for att in el:
        if att == 'attName':
            counter +=1
            #print att
"""
def get_db(db_name):
    from pymongo import MongoClient
    client = MongoClient('localhost:27017')
    db = client[db_name]
    return db


db = get_db('test')
#mongoDB
db.MYK.aggregate(pipeline)
#no of cities
pipeline = [{'$match': {'address.city': {'$exists': True}}},
            {'$group': {'_id':{'city': '$address.city'}}}]
            
#no of postcodes:
pipeline = [{'$match': {'address.postcode': {'$exists': True}}},
            {'$group': {'_id':{'postcode': '$address.postcode'}}}]

#no of postcodes appearing in more than one city:
pipeline = [{'$match': {'address.city': {'$exists': True},
                        'address.postcode': {'$exists': True}}},
            {'$group': {'_id':{'city': '$address.city', 'postcode': '$address.postcode'}}},
            {'$group': {'_id': '$_id.postcode',
                        'count': {'$sum': 1}}},
            {'$match': {'count': {'$gt': 1}}}]
            
#cities with postcode = '56745'
pipeline = [{'$match': {'address.city': {'$exists': True},
                        'address.postcode': '56745'}},
            {'$group': {'_id':{'city': '$address.city', 'postcode': '$address.postcode'}}}]
            
#no of cities having more than one postcode:
pipeline = [{'$match': {'address.city': {'$exists': True},
                        'address.postcode': {'$exists': True}}},
            {'$group': {'_id':{'city': '$address.city', 'postcode': '$address.postcode'}}},
            {'$group': {'_id': '$_id.city',
                        'count': {'$sum': 1}}},
            {'$match': {'count': {'$gt': 1}}}]

#no any kind of contact information is given
pipeline = [{'$match': {'contact': {'$exists': True}}}]


pipeline = [{'$match': {'pos': {'$exists': True}}},
            {'$unwind': '$pos'}]


#Get result for each contact information
for ContactMode in ['email', 'fax', 'google_plus', 'mobile', 'office', 'phone', 'website']:
    pipeline = [{'$match': {'contact.' + ContactMode: {'$exists': True}}}]
    print ContactMode, len(list(db.MYK.aggregate(pipeline)))

#Get results for names given in different languages
for Name in ['name', 'name:de','name:en','name:es','name:fr','name:hu','name:it','name:nl','name:ru','name:sk']:
    pipeline = [{'$match': {Name: {'$exists': True}}}]
    print Name, len(list(db.MYK.aggregate(pipeline)))

