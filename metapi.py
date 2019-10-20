#! /usr/bin/env python3

import requests
import fire
import pandas as pd
import sqlite3
import json
import os
from tqdm import tqdm
from os import listdir
import subprocess
import shlex
import sys
import configparser


class MetAPI():

    def __init__(self):
        
        # Check env for home -- needed so app can run from anywhere
        try:
            self.home = os.environ['METAPI_HOME']
            self.pub = os.environ['METAPI_PUB']
        except KeyError:
            print('No METAPI_HOME and/or METAPI_PUB set.')
            sys.exit()
        
        # Load Config -- we put paths and URLs here
        self.cfg = configparser.ConfigParser()
        self.cfg.read(self.home + '/config.ini')
        
        # Get Met base URL
        self.base_url = self.cfg.get('Met', 'base_url')
        
        # Load Data Dictionary
        try:
            ddfile = open(self.home + '/datadict.csv', 'r').readlines()
            self.dd = pd.DataFrame([row.split('\t') for row in ddfile], 
                columns=['col', 'type', 'desc', 'example'])
            self.ddcols = [col.strip() for col in self.dd.col.tolist()]
        except:
            print('Could not open or parse data dictionary.')
            
        # Initialize objects dataframe
        self.odf = pd.DataFrame(columns=self.dd.col.values)
        
        # Open database (for metadata)
        try:
            self.db = sqlite3.connect(self.pub + '/' + self.cfg.get('DEFAULT', 'db_name'))
        except sqlite3.OperationalError as e:
            print(e)
            
        # Define image directory (for downloads)
        self.imgdir = self.pub + '/' + self.cfg.get('DEFAULT', 'images_subdir')
        self.smallimgdir = self.pub + '/' + self.cfg.get('DEFAULT', 'small_images_subdir')
        
    def __del__(self):
        try:
            self.db.close()
        except:
            print("Looks like there's no db to close.")

    def test(self):
        print('cfg', self.cfg.sections())
        print('METAPI_PUB', self.pub)
        print('METAPI_HOME', self.home)
        print('ddcols', self.ddcols)
        print('base_url', self.base_url)

    def create_table(self, drop=False):
        """Create table for objects in database"""
        df = pd.DataFrame(columns=self.ddcols)
        df.objectID = df.objectID.astype('int')  
        df = df.set_index('objectID')
        df.to_sql('object', self.db)

    def get_all_object_ids(self, overwrite=True):
        """Populate database with objectIDs"""
        url = self.base_url + '/objects'
        r = requests.get(url)
        js = json.loads(r.text)
        total = js['total']
        oids = js['objectIDs']
        sql = "INSERT INTO object (objectID) VALUES (?)"
        self.db.executemany(sql, [(oid,) for oid in oids])
        self.db.commit()
        
    def get_remaining_oids(self):
        """Get remaining OIDs from database"""
        objects = pd.read_sql('select objectID from object where title is null order by objectID', 
            self.db, index_col='objectID')
        oids = objects.index.tolist()
        return oids

    def get_object_metadata(self, oids=None):
        """Get object metadata for list of oids"""
        if not oids:
            oids = self.get_remaining_oids()

        print(len(oids), 'to download')
        est_hours = (len(oids) * 1.5) / 60 / 60 
        print('Est dl time in hours:', est_hours)

        set_str = ', '.join(["{} = ?".format(col) for col in self.ddcols[1:]])
        sql = "UPDATE object SET {} WHERE objectID = ?".format(set_str)
        sql_err = "UPDATE object SET title = '__NO_URL__' WHERE objectID = ?"
        
        print('Starting from objectID', oids[0])
        for oid in tqdm(oids):
            url ='{}/objects/{}'.format(self.base_url, oid)
            try:
                r = requests.get(url)
            except requests.exceptions.ConnectionError:
                print("Can't access {}; moving on.".format(oid))
                self.db.execute(sql_err, (oid,))
                self.db.commit()
                continue
            py = json.loads(r.text)
            try:
                row = [str(py[key]) for key in self.ddcols]
                row = row[1:] + row[0:1]
                self.db.execute(sql, row)
                self.db.commit()
            except KeyError as e:
                print(e)

    def download_oid_image(self, row):
        oid = row.objectID
        imgs = [row.primaryImage] + eval(row.additionalImages)
        for i, url in enumerate(imgs):
            outfile = "{}-{}.jpg".format(oid, i)
            cmd = shlex.split("wget -qN \"{}\" --output-document={}".format(url, outfile))
            r = subprocess.run(cmd, cwd=self.imgdir)

    def download_images(self):
        images = [img for img in listdir(self.imgdir) if img.endswith('.jpg')]
        oids_done = [0] + [int(oid.split('/')[-1].split('-')[0]) for oid in images]
        max_oid = max(oids_done)
        sql = "select objectID, primaryImage, additionalImages from object where primaryImage like '%jpg' and objectID >= ?"
        df = pd.read_sql(sql, self.db, params=(max_oid,))
        #for row in tqdm(df.iterrows()):
        #    self.download_oid_image(row[1])
        for i in tqdm(range(df.shape[0])):
            row = df.iloc[i]
            self.download_oid_image(row)
            
    def download_small_image(self, row):
        oid = row.objectID
        url = row.primaryImageSmall
        outfile = "{}-small.jpg".format(oid)
        cmd = shlex.split("wget -qN \"{}\" --output-document={}".format(url, outfile))
        r = subprocess.run(cmd, cwd=self.smallimgdir)

    def download_small_tagged_images(self):
        images = [img for img in listdir(self.smallimgdir) if img.endswith('.jpg')]
        oids_done = [0] + [int(oid.split('/')[-1].split('-')[0]) for oid in images]
        max_oid = max(oids_done)
        sql = """select objectID, primaryImageSmall 
            from object 
            where primaryImageSmall like '%jpg' 
            and objectID >= ?
            and tags != '[]' """
        df = pd.read_sql(sql, self.db, params=(max_oid,))
        for i in tqdm(range(df.shape[0])):
            row = df.iloc[i]
            # self.download_small_image(row)
            oid = row.objectID
            url = row.primaryImageSmall
            outfile = "{}-small.jpg".format(oid)
            cmd = shlex.split("wget -qN \"{}\" --output-document={}".format(url, outfile))
            r = subprocess.run(cmd, cwd=self.smallimgdir)

        
if __name__ == '__main__':
    
    mapi = MetAPI()
    fire.Fire(mapi)
