#! /usr/bin/env python3

import requests
import fire
import pandas as pd
import sqlite3
import json
import os
from tqdm import tqdm

"""
From https://metmuseum.github.io/

Objects: A listing of all valid Object IDs available for access.
Object: A record for an object, containing all open access data about that object, including its image (if the image is available under Open Access)
Departments: A listing of all valid departments, with their department ID and the department display name
Search: A listing of all Object IDs for objects that contain the search query within the object’s data
"""

class MetAPI():

    base_url = 'https://collectionapi.metmuseum.org/public/collection/v1'
    data_dict = """
objectID 	int 	Identifying number for each artwork (unique, can be used as key field) 	437133
isHighlight 	boolean 	When "true" indicates a popular and important artwork in the collection 	Vincent van Gogh's "Wheat Field with Cypresses"
accessionNumber 	string 	Identifying number for each artwork (not always unique) 	67.241
isPublicDomain 	boolean 	When "true" indicates an artwork in the Public Domain 	Vincent van Gogh's "Wheat Field with Cypresses"
primaryImage 	string 	URL to the primary image of an object in JPEG format 	https://images.metmuseum.org/CRDImages/ep/original/DT1567.jpg
primaryImageSmall 	string 	URL to the lower-res primary image of an object in JPEG format 	https://images.metmuseum.org/CRDImages/ep/web-large/DT1567.jpg
additionalImages 	array 	An array containing URLs to the additional images of an object in JPEG format 	["https://images.metmuseum.org/CRDImages/ep/original/LC-EP_1993_132_suppl_CH-004.jpg", "https://images.metmuseum.org/CRDImages/ep/original/LC-EP_1993_132_suppl_CH-003.jpg", "https://images.metmuseum.org/CRDImages/ep/original/LC-EP_1993_132_suppl_CH-002.jpg", "https://images.metmuseum.org/CRDImages/ep/original/LC-EP_1993_132_suppl_CH-001.jpg"]
constituents 	array 	An array containing the constituents of an object, with both an artist name and their role 	[{"role":"Artist","name":"Vincent van Gogh"}]
department 	string 	Indicates The Met's curatorial department responsible for the artwork 	437133
objectName 	string 	Describes the physical type of the object 	Dress, Painting, Photograph, Vase
title 	string 	Title, identifying phrase, or name given to a work of art 	Wheat Field with Cypresses
culture 	string 	Information about the culture, or people from which an object was created 	Afgan, British, North African
period 	string 	Time or time period when an object was created 	Ming dynasty (1368-1644), Middle Bronze Age
dynasty 	string 	Dynasty (a succession of rulers of the same line or family) under which an object was created 	Kingdom of Benin, Dynasty 12
reign 	string 	Reign of a monarch or ruler under which an object was created 	Amenhotep III, Darius I, Louis XVI,
portfolio 	string 	A set of works created as a group or published as a series. 	Birds of America, The Hudson River Portfolio, Speculum Romanae Magnificentiae
artistRole 	string 	Role of the artist related to the type of artwork or object that was created 	Artist for Painting, Designer for Dress
artistPrefix 	string 	Describes the extent of creation or describes an attribution qualifier to the information given in the artistRole field 	In the Style of, Possibly by, Written in French by
artistDisplayName 	string 	Artist name in the correct order for display 	Vincent van Gogh
artistDisplayBio 	string 	Nationality and life dates of an artist, also includes birth and death city when known. 	Dutch, Zundert 1853–1890 Auvers-sur-Oise
artistSuffix 	string 	Used to record complex information that qualifies the role of a constituent, e.g. extent of participation by the Constituent (verso only, and followers) 	or, workshop, for, verso only
artistAlphaSort 	string 	Used to sort artist names alphabetically. Last Name, First Name, Middle Name, Suffix, and Honorific fields, in that order. 	Gogh, Vincent van
artistNationality 	string 	National, geopolitical, cultural, or ethnic origins or affiliation of the creator or institution that made the artwork 	Spanish; Dutch; French, born Romania
artistBeginDate 	string 	Year the artist was born 	1840
artistEndDate 	string 	Year the artist died 	1926
objectDate 	string 	Year, a span of years, or a phrase that describes the specific or approximate date when an artwork was designed or created 	1865–67, 19th century, ca. 1796
objectBeginDate 	string 	Machine readable date indicating the year the artwork was started to be created 	1867, 1100, -900
objectEndDate 	string 	Machine readable date indicating the year the artwork was completed (may be the same year or different year than the objectBeginDate) 	1888, 1100, -850
medium 	string 	Refers to the materials that were used to create the artwork 	Oil on canvas, Watercolor, Gold
dimensions 	string 	Size of the artwork or object 	16 x 20 in. (40.6 x 50.8 cm)
creditLine 	string 	Text acknowledging the source or origin of the artwork and the year the object was acquired by the museum. 	Robert Lehman Collection, 1975
geographyType 	string 	Qualifying information that describes the relationship of the place catalogued in the geography fields to the object that is being catalogued 	Made in, From, Attributed to
city 	string 	City where the artwork was created 	New York, Paris, Tokyo
state 	string 	State or province where the artwork was created, may sometimes overlap with County 	Alamance, Derbyshire, Brooklyn
county 	string 	County where the artwork was created, may sometimes overlap with State 	Orange County, Staffordshire, Brooklyn
country 	string 	Country where the artwork was created or found 	China, France, India
region 	string 	Geographic location more specific than country, but more specific than subregion, where the artwork was created or found (frequently null) 	Bohemia, Midwest, Southern
subregion 	string 	Geographic location more specific than Region, but less specific than Locale, where the artwork was created or found (frequently null) 	Malqata, Deir el-Bahri, Valley of the Kings
locale 	string 	Geographic location more specific than subregion, but more specific than locus, where the artwork was found (frequently null) 	Tomb of Perneb, Temple of Hatshepsut, Palace of Ramesses II
locus 	string 	Geographic location that is less specific than locale, but more specific than excavation, where the artwork was found (frequently null) 	1st chamber W. wall; Burial C 2, In coffin; Pit 477
excavation 	string 	The name of an excavation. The excavation field usually includes dates of excavation. 	MMA excavations, 1923–24; Khashaba excavations, 1910–11; Carnarvon excavations, 1912
river 	string 	River is a natural watercourse, usually freshwater, flowing toward an ocean, a lake, a sea or another river related to the origins of an artwork (frequently null) 	Mississippi River, Nile River, River Thames
classification 	string 	General term describing the artwork type. 	Basketry, Ceramics, Paintings
rightsAndReproduction 	string 	Credit line for artworks still under copyright. 	© 2018 Estate of Pablo Picasso / Artists Rights Society (ARS), New York
linkResource 	string 	URL to object's page on metmuseum.org 	https://www.metmuseum.org/art/collection/search/547802
metadataDate 	datetime 	Date metadata was last updated 	2018-10-17T10:24:43.197Z
repository 	string 		Metropolitan Museum of Art, New York, NY
objectURL 	string 	URL to object's page on metmuseum.org 	https://www.metmuseum.org/art/collection/search/547802
tags 	array 	An array of subject keyword tags associated with the object 	["Architecture","Temples","Hieroglyphs"]
""".split('\n')[1:-1]

    # Create dataframe to store column data dictionary
    dd = pd.DataFrame([row.split('\t') for row in data_dict], columns=['col', 'type', 'desc', 'example'])
    ddcols = [col.strip() for col in dd.col.tolist()]

    def __init__(self, pwd='./', dbname='mapi.db'):
        self.pwd = pwd
        self.odf = pd.DataFrame(columns=self.md.col.values)
        self.db = sqlite3.connect(pwd+dbname)
        
    def create_table(self, drop=False):
        df = pd.DataFrame(columns=self.ddcols)
        df.objectID = df.objectID.astype('int')  
        df = df.set_index('objectID')
        df.to_sql('object', self.db)

    def get_all_object_ids(self, overwrite=True):
        url = self.base_url + '/objects'
        r = requests.get(url)
        js = json.loads(r.text)
        total = js['total']
        oids = js['objectIDs']
        sql = "INSERT INTO OBJECT (objectID) VALUES (?)"
        self.db.executemany(sql, [(oid,) for oid in oids])
        self.db.commit()

    def get_object_metadata(self):
        set_str = ', '.join(["{} = ?".format(col) for col in self.ddcols[1:]])
        sql = "UPDATE object SET {} WHERE objectID = ?".format(set_str)
        oid_n = pd.read_sql('select count(objectID) as n from object', self.db)
        print('Count', oid_n.n.values[0])
        objects = pd.read_sql('select * from object', self.db, index_col='objectID')
        for oid in tqdm(objects.index.tolist()):
            url ='{}/objects/{}'.format(self.base_url, oid)
            r = requests.get(url)
            js = r.text
            py = json.loads(js)
            row = [str(py[key]) for key in py.keys()]
            row = row[1:] + row[0:1]
            self.db.execute(sql, row)
            self.db.commit()

if __name__ == '__main__':
    
    mapi = MetAPI()
    fire.Fire(mapi)