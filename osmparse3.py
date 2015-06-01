import xml.etree.cElementTree as et
import pprint
import re
import codecs
import json
import pymongo as pim

c=pim.MongoClient()
db=pim.database.Database(c,'jburg')
db.staging2.drop()
pim.collection.Collection(db,'staging2',create=True)

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]


def shape_element(element):
    node = {} #this is the dictionary that will turn into the document we putinto the database
    created={}#the created subdocument in the data model
    if element.tag == "node" or element.tag == "way" : #runs the code if it is a node or way xml element
        src=element.attrib #this is the dictionary of the attributes
        node['type']=element.tag 


        for i in CREATED:
            created[i]=src[i] #populates the created subdocument
            src.pop(i,None) #removes the attribute from the dictionary so it doesnt get re added into the parent document
        if len(created)!=0:
            node['created']=created # attaches the subdocument to the parent document

        node['pos']=[]
        if element.get('lat'):
            node['pos'].append(float(element.get('lat'))) 
            src.pop('lat',None)
        if element.get('lon'):
            node['pos'].append(float(element.get('lon')))
            src.pop('lon',None)
        if len(node['pos'])==0:#if there is no lat or lon data it takes pos array out of the parent document
            node.pop('pos',None)



        for i in src: #attaches the remaining attributes to the document
            node[i]=src[i]


        node['address']={} #creates a dict to make the address subdocument
        
        for i in element.iter('tag'):
            if re.search(problemchars,i.get('k')): #skips problem characters
                pass


            else:
                if 'addr' in i.get('k'):#adds the address information where available
                    j=i.get('k').split(':')
                    if len(j)==2:
                        node['address'][j[1]]=translate(i.get('v'))#creates the translation for troubled data entries int the address
                elif i.get('k'):
                    node[i.get('k')]=i.get('v')


        if len(node['address'])==0: 
            node.pop('address',None) #removes address subdocument if it is empty


        nd=[]
        if element.tag=='way':
            for j in element.iter('nd'):
                nd.append(j.get('ref'))
        if len(nd)!=0:
            node['node_refs']=nd #adds the nd attributes of a way in an array to the final document
        return node
    else:
        pass


def translate(st): #translation dictionary
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
    
    

def replace(l, X, Y): #replacement method in an array
    for i,v in enumerate(l):
        if v == X:
            l.pop(i)
            l.insert(i, Y)

#the Method to insert into the databasea
for _, element in et.iterparse('/Users/CPMcIntyre/Documents/UdacityDataAnalyst/DataWrangling-Mongodb/osmdb/johannesburg_south-africa.osm'):
    k=shape_element(element)
    if k!= None:
        db.staging2.insert_one(k)


