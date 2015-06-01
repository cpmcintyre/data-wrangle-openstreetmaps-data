import xml.etree.cElementTree as et
import pprint
import re
import codecs
import json
import pymongo as pim



#######TO THE GRADER: THIS IS THE SAME AS THE OTHER PYTHON SCRIPT EXCEPT THAT I CHANGED THE DATA MODEL
#######THE CHANGE JUST LOADED LAT AND LON AS A SUBDOCUMENT SO I COULD GET TO THEM IN THE QUERIES MORE EASILY
#######ILL MARK IT BELOW


c=pim.MongoClient()
db=pim.database.Database(c,'jburg')
db.staging3.drop()
pim.collection.Collection(db,'staging3',create=True)

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]


def shape_element(element):
    node = {}
    created={}
    if element.tag == "node" or element.tag == "way" :
        src=element.attrib
        node['type']=element.tag
        for i in CREATED:
            created[i]=src[i]
            src.pop(i,None)
        if len(created)!=0:
            node['created']=created
        node['pos']={}
        if element.get('lat'):
            node['pos']['lat']=float(element.get('lat'))#####HERE IS THE CHANGE
            src.pop('lat',None)
        if element.get('lon'):
            node['pos']['lon']=float(element.get('lon'))#####HERE IS THE OTHER CHANGEs
            src.pop('lon',None)
        if len(node['pos'])==0:
            node.pop('pos',None)
        for i in src:
            node[i]=src[i]
        node['address']={}
        for i in element.iter('tag'):
            if re.search(problemchars,i.get('k')):
                print('bad data')
                pass
            else:
                if 'addr' in i.get('k'):
                    j=i.get('k').split(':')
                    if len(j)==2:
                        node['address'][j[1]]=translate(i.get('v'))
                elif i.get('k'):
                    node[i.get('k')]=i.get('v')
        if len(node['address'])==0:
            node.pop('address',None)
        nd=[]
        if element.tag=='way':
            for j in element.iter('nd'):
                nd.append(j.get('ref'))
        if len(nd)!=0:
            node['node_refs']=nd                
        return node
    else:
        pass


def translate(st):
    trdict={'Ave':'Avenue',
            'ave':'Avenue',
            'Ave':'Avenue',
            'ave.':'Avenue',
            'road':'Road',
            'rd':'Road',
            'rd.':'Road',
            'Zuid-Afrika':'South Africa',
            'ZA':'South Africa'}
    
    k=''

    if type(st)==str:        
        j=st.split(' ')
        for i in j:
            if i in trdict:
                j=replace(j,i,trdict[i])
        try:

            if len(j)==1:
                return j[0]
            else:
                for i in j:
                    k=k+i+' '
            return k
        except:
            pass
    else:
        pass
    
    

def replace(l, X, Y):
    for i,v in enumerate(l):
        if v == X:
            l.pop(i)
            l.insert(i, Y)


for _, element in et.iterparse('/Users/CPMcIntyre/Documents/UdacityDataAnalyst/DataWrangling-Mongodb/osmdb/johannesburg_south-africa.osm'):
    k=shape_element(element)
    if k!= None:
        db.staging3.insert_one(k)


