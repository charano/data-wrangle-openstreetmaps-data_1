try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET
import pprint
import re
import codecs
import json

lower = re.compile(r'^([a-z]|_)*$')
lower_colon = re.compile(r'^([a-z]|_)*:([a-z]|_)*$')
problemchars = re.compile(r'[=\+/&<>;\'"\?%#$@\,\. \t\r\n]')
tiger_tags = re.compile(r'tiger:[a-zA-Z-0-9_]*')
gnis_tags = re.compile(r'gnis:[a-zA-Z-0-9_]*')
long_zip_re = re.compile(r'[0-9]{5}-?[0-9]{4}$')
correct_zip_re =  re.compile(r'[0-9]{5}$')
ga_zip_re =  re.compile(r'GA [0-9]{5}$')

CREATED = [ "version", "changeset", "timestamp", "user", "uid"]

mapping = { "St": "Street",
            "St.": "Street",
            "Av": "Avenue",
            "Ave": "Avenue",
            "Rd.": "Road",
            "AVENUE": "Avenue",
            "Ave.": "Avenue",
            "Bldv": "Boulevard",
            "Blvd": "Boulevard",
            "Cir": "Circle",
            "Ct" : "Court",
            "Ct." : "Court",
            "Dr" : "Drive",
            "Hwy": "Highway",
            "Ln" : "Lane",
            "Pl" : "Place",
            "Pkwy": "Parkway",
            "RD" : "Road",
            "ROAD" : "Road",
            "Rd" : "Road",
            "Rd." : "Road",
            "Trce" : "Trace",
            "Trl" : "Trail",
            "Xing" : "Crossing",
            "blvd" : "Boulevard",
            "circle" : "Circle",
            "dr" : "Drive",
            "lane" : "Lane",
            "parkway" : "Parkway",
            "E": "East",
            "W": "West",
            "NW": "Northwest",
            "SW": "Southwest",
            "NE": "Northeast",
            "NW": "Northwest"
            }

def shape_element(element):
    node = {}
    
    if element.tag == "node" or element.tag == "way" :
        created_node = {}
        pos = [None, None]
        if element.tag == "node":
            node["type"] = "node"
        else:
            node["type"] = "way"
        for at in element.attrib:
            if at in CREATED:
                created_node[at] = element.attrib[at]
            elif at == "lat" and element.attrib[at] <> None:
                pos[0] = float(element.attrib[at])
            elif at == "lon" and element.attrib[at] <> None:
                pos[1] = float(element.attrib[at])
            else:
                node[at] = element.attrib[at]
        node["created"] = created_node
        if pos[0] <> None and pos[1] <> None:
            node["pos"] = pos

        address = dict()
        node_refs = []
        
        for child in element:
            #Only check keys with valid keys and ignore ones from the Tiger and GIS projects
            if "k" in child.attrib and problemchars.match(child.attrib['k']) == None and tiger_tags.match(child.attrib['k']) == None and gnis_tags.match(child.attrib['k']) == None:
                #get the street name
                if child.attrib['k'] == "addr:street":
                    address["street"] = child.attrib['v']
                #otherwise, get any other addr components
                elif "addr:" in child.attrib['k']:
                    components = child.attrib['k'].split(":")
                    address[components[1]] = child.attrib['v']
                #get all remaining keys
                else:
                    node[child.attrib['k']] = child.attrib['v']
            #find the ref tags
            elif "ref" in child.attrib:
                node_refs.append(child.attrib['ref'])
        if "street" in address:
            words = address["street"].split()
            for word in words:
                for street_type in mapping:
                    if street_type == word:
                        address["street"] = address["street"].replace(street_type, mapping[street_type])
                        break
        if "postcode" in address:
            #Check to see if it is a correctly formatted zipcode
            if correct_zip_re.match(address["postcode"]) <> None:
                address["postcode"] = address["postcode"]
            #fixing 5+4 digit zip codes
            elif long_zip_re.match(address["postcode"]) <> None:
                address["postcode"] = address["postcode"][:5]
            #fixing "GA " zip codes
            elif ga_zip_re.match(address["postcode"]) <> None:
                address["postcode"] = address["postcode"][4:]
            #If it does not match any of the above, set it to None
            else:
                address.pop("postcode", None)
        if address:
            node["address"] = address
        if node_refs:
            node["node_refs"] = node_refs
        return node
    else:
        return None


def process_map(file_in, pretty = False):
    # You do not need to change this file
    file_out = "{0}.json".format(file_in)
    data = []
    with codecs.open(file_out, "w") as fo:
        for _, element in ET.iterparse(file_in):
            el = shape_element(element)
            if el:
                data.append(el)
                if pretty:
                    fo.write(json.dumps(el, indent=2)+"\n")
                else:
                    fo.write(json.dumps(el) + "\n")
        element.clear()
    return data

if __name__ == "__main__":
    data = process_map('atlanta_georgia.osm', True)
    print 'Done'
    #pprint.pprint(data[0:100])