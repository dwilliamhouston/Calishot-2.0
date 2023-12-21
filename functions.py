import requests
from pathlib import Path
from urllib.parse import *
import uuid
from sqlite_utils import Database
import datetime
import gevent
from gevent import monkey
from gevent import Timeout
from gevent.pool import Pool
import pandas as pd
from sqlite_utils import Database
import sqlite3
import sys
import shodan
import os
import time
import re
import shutil
from typing import Dict
import json
from humanize import naturalsize as hsize
import humanize
from langid.langid import LanguageIdentifier, model
import iso639
import time
import json
import unidecode

from requests.adapters import HTTPAdapter
import urllib.parse
import urllib3
from pathlib import Path
import uuid
from sqlite_utils import Database

import gevent
from gevent import monkey
from gevent import Timeout
from gevent.pool import Pool

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)
SHODAN_API_KEY = "Enter SHODAN API KEY here from your Shodan Account"
global api
api = shodan.Shodan(SHODAN_API_KEY)

global site_conn
site_conn = sqlite3.connect("sites.db")
site_cursor = site_conn.cursor()
# monkey.patch_socket()

########################
# Setup Sites Database #
########################
def init_sites_db(dir="."):
    
    path = Path(dir) / "sites.db" 

    db = Database(path)
    if not "sites" in db.table_names():
        db["sites"].create({
        "uuid": str,
        "url": str,
        "hostnames": str,
        "ports": str,
        "country": str,
        "isp": str,
        "status": str,
        "last_online": str,
        "last_check": str,
        "error": int,
    #     "schema_version": 1
    #     # TODO: add the most common formats
        }, pk="uuid")
        # }, pk="uuid", not_null=True)

    # if not "sites" in db.table_names():
    #     db["sites"].create({
    #     "uuid": str
    #     }, pk="uuid",)

    db.table("sites", pk='uuid', batch_size=100, alter=True)
    return db

########################################
# Save sites found into Sites Database #
########################################
def save_site(db: Database, site):
    # # TODO: Check if the site is not alreday present
    # def save_sites(db, sites):
    #     db["sites"].insert_all(sites, alter=True,  batch_size=100)
    if not 'uuid' in site: 
        site['uuid']=str(uuid.uuid4())    
    print(site)
    db["sites"].upsert(site, pk='uuid')


##########################
# Validate Site and save #
##########################
def check_and_save_site(db, site):
        res= check_calibre_site(site)
        print(res)
        save_site(db, res)

# import pysnooper
# @pysnooper.snoop()

######################
# Check Calibre Site #
######################
def check_calibre_site(site):
    ret={}
    ret['uuid']=site["uuid"]
    now=str(datetime.datetime.now())
    ret['last_check']=now 

    api=site['url']+'/ajax/'
    timeout=15
    library=""
    url=api+'search'+library+'?num=0'
    print()
    print("Getting ebooks count:", site['url'])
    print(url)
    
    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        r.status_code
        ret['error']=r.status_code
        if (r.status_code == 401):
            ret['status']="unauthorized"
        else:
            ret['status']="down"
        return ret
    except requests.RequestException as e: 
        print("Unable to open site:", url)
        # print (getattr(e, 'message', repr(e)))
        print (e)
        ret['status']="down"
        return ret
    except Exception as e:
        print ("Other issue:", e)
        ret['status']='Unknown Error'
        print (e)
        return ret
    except :
        print("Wazza !!!!")
        ret['status']='Critical Error'
        print (e)
        return ret

    try: 
        print("Total count=",r.json()["total_num"])
    except:
        pass

    status=ret['status']='online'
    if status=="online":
        ret['last_online']=now 

    return ret

######################
# Get UUID from Site #
######################
def get_site_uuid_from_url(db, url):

    site=urlparse(url)
    hostname=site.hostname
    site=site._replace(path='')
    
    url=urlunparse(site)
    # print (url)

    # print (hostname)
    row=db.conn.execute(f"select * from sites where instr(hostnames, '{hostname}')").fetchone()
    # print(row)
    if row:
        return row

##############################
# Get URL, hostname and Port #
##############################
def map_site_from_url(url):
    ret={}

    site=urlparse(url)

    print(site)
    site=site._replace(path='')
    ret['url']=urlunparse(site)
    ret['hostnames']=[site.hostname] 
    ret['ports']=[str(site.port)]
    return ret

############################################################
# Import the URLS from the temp file and write to Database #
############################################################
def import_urls_from_file(filepath, dir='.'):

    #TODO skip malformed urls
    #TODO use cache instead

    db=init_sites_db(dir)

    with open(filepath) as f:
        for url in f.readlines():
            url=url.rstrip()
            # url='http://'+url
            if get_site_uuid_from_url(db, url):
                print(f"'{url}'' already present")
                continue
            print(f"'{url}'' added")
            save_site(db, map_site_from_url(url))
    
###################################
# Get list of libraries from site #
###################################
def get_libs_from_site(site):

    server=site.rstrip('/')
    api=server+'/ajax/'
    timeout=30
    
    print()
    print("Server:", server)
    url=api+'library-info'

    print()
    print("Getting libraries from", server)
    # print(url)

    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.RequestException as e: 
        print("Unable to open site:", url)
        # return
    except Exception as e:
        print ("Other issue:", e)
        return
        # pass

    libraries = r.json()["library_map"].keys()
    print("Libraries:", ", ".join(libraries))
    return libraries

###################################
# Check the list of sites in file #
###################################
def check_calibre_list(dir='.'):    
    db=init_sites_db(dir)
    sites=[]
    for row in db["sites"].rows:
        print(f"Queueing:{row['url']}")
        sites.append(row)
    print(sites)
    pool = Pool(100)
    pool.map(lambda s: check_and_save_site (db, s), sites)

#################
# Get site UUID #
#################
# example of a fts search sqlite-utils index.db "select * from summary_fts where summary_fts  match 'title:fre*'"
def get_site_db(uuid, dir):
        f_uuid=str(uuid)+".db"
        print(f_uuid)
        path = Path(dir) / str(f_uuid) 
        return Database(path)

############################
# Initialize Site Database #
############################
def init_site_db(site, _uuid="", dir="."):
    
    if not _uuid:
        s_uuid=str(uuid.uuid4())
    else:
        s_uuid=str(_uuid)

    f_uuid=s_uuid+".db"
    path = Path(dir) / f_uuid 
    db = Database(path)


    if not "site" in db.table_names():
        s=db["site"]
        s.insert(
            {    
                "uuid": s_uuid,
                "urls": [site],
                "version": "",
                "major": 0,
                "schema_version": 1,
            }
            , pk='uuid'
        )

    if not "ebooks" in db.table_names():
        db["ebooks"].create({
        "uuid": str,
        "id": int,
        "library": str,  #TODO: manage libraries ids as integer to prevent library renam on remote site  
        "title": str,
        "authors": str,
        "series": str,
        "series_index": int,
        # "edition": int, 
        "language": str,
        "desc": str,
        "identifiers": str,
        "tags": str,
        "publisher": str,
        "pubdate": str,
        "last_modified": str,
        "timestamp": str,
        "formats": str,
        "cover": int,
        # "epub": int,
        # "mobi": int,
        # "pdf": int,
        # TODO: add the most common formats to avoid alter tables
        }, pk="uuid")

    if not "libraries" in db.table_names():
        db["libraries"].create({
        "id": int,    
        "names": str
        }, pk="id")
        # db.table("ebooks", pk="id")
        # db.table("ebooks", pk="id", alter=True
    return db

#################################
# Get Library URL from Database #
#################################
def get_format_url(db, book, format):
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]
    library=book['library']
    id_=str(book['id'])

    f_url = url+"/get/"+format+"/"+id_+"/"+library
    return f_url
    
############################
# Get Library Version Info #
############################
def get_desc_url(db, book):
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]

    library=book['library']
    id_=str(book['id'])

    f_urls=[]

    major=  list(db['site'].rows)[0]["major"]

    if major >= 3:
        d_url =url+"#book_id="+id_+"&library_id="+library+"&panel=book_details"
    else:
        d_url =url+"/browse/book/"+id_

    return d_url

###############################
# Write book info to Database #
###############################
def save_books_metadata_from_site(db, books):
    uuid = list(db['site'].rows)[0]["uuid"]
    # print(uuid)
    ebooks_t=db["ebooks"]
    # print([c[1] for c in ebooks_t.columns])
    # for b in books:
    #     print(b['title'])
    #     ebooks_t.insert(b, alter=True)
    # ebooks_t.insert_all(books, alter=True)
    ebooks_t.insert_all(books, alter=True,  pk='uuid', batch_size=1000)
    # print([c[1] for c in ebooks_t.columns])

##########
# Unused #
##########
def load_metadata(dir, uuid):
    pass

##########################################
# Update Status when book details loaded #
##########################################
def update_done_status(book):
    source=book['source']
    if source['status']!='ignored':
        if set(source['formats'].keys()) == set(book['formats']) & set(source['formats'].keys()):
            book['source']['status']="done"
        else: 
            book['source']['status']="todo"

################################
# Index the Site List Sequence #
################################
def index_site_list_seq(file):
    with open(file) as f:
        for s in f.readlines():
            # try: 
            #     index_ebooks(s.rstrip())
            # except:
            #     continue
            index_ebooks(s.rstrip())

###################
# Index Site List #
###################
def index_site_list(file):
    pool = Pool(40)

    with open(file) as f:
        sites = f.readlines()
        sites= [s.rstrip() for s in sites]
        print(sites)
        pool.map(index_ebooks_except, sites)

##########################
# Index ebooks Exception #
##########################
def index_ebooks_except(site):
    try:
        index_ebooks(site)
    except:
        print("Error on site")

################
# Index Ebooks #
################
def index_ebooks(site, library="", start=0, stop=0, dir=".", num=1000, force_refresh=False):

    #TODO old calibres don't manage libraries.  /ajax/library-info endpoint doesn't exist. It would be better to manage calibre version directly 

    libs=[]
    try:
        libs= get_libs_from_site(site)
    except:
        print("old lib")
        
    _uuid=str(uuid.uuid4())
    
    if libs:
        for lib in libs:
            index_ebooks_from_library(site=site, _uuid=_uuid, library=lib, start=start, stop=stop, dir=dir, num=num, force_refresh=force_refresh)   
    else:
            index_ebooks_from_library(site=site, _uuid=_uuid, start=start, stop=stop, dir=dir, num=num, force_refresh=force_refresh)   

#############################
# Index Ebooks from Library #
#############################
def index_ebooks_from_library(site, _uuid="", library="", start=0, stop=0, dir=".", num=1000, force_refresh=False):
    
    offset= 0 if not start else start-1
    num=min(1000, num)
    server=site.rstrip('/')
    api=server+'/ajax/'
    lib=library
    library= '/'+library if library else library

    timeout=15

    print(f"\nIndexing library: {lib} from server: {server} ")
    url=api+'search'+library+'?num=0'
    print(f"\nGetting ebooks count of library: {lib} from server:{server} ")
    # print(url)
    
    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.RequestException as e: 
        print("Unable to open site:", url)
        return
        # pass
    except Exception as e:
        print ("Other issue:", e)
        return
        # pass
    except :
        print("Wazza !!!!")
        sys.exit(1)
        

    total_num=int(r.json()["total_num"])
    total_num= total_num if not stop else stop
    print()    
    print(f"Total count={total_num} from {server}")
 
    # library=r.json()["base_url"].split('/')[-1]
    # base_url=r.json()["base_url"]

    # cache_db=init_cache_db(dir=dir)
    # _uuid=get_uuid_from_url(cache_db)
    db=init_site_db(site, _uuid=_uuid, dir=dir)
    r_site = (list(db['site'].rows)[0])

    r_site['version']=r.headers['server']
    r_site['major']=int(re.search('calibre.(\d).*', r.headers['server']).group(1))
    db["site"].upsert(r_site, pk='uuid')

    print()

    range=offset+1
    while offset < total_num:
        remaining_num = min(num, total_num - offset)
        # print()
        # print("Downloading ids: offset="+str(offset), "num="+str(remaining_num))
        print ('\r {:180.180}'.format(f'Downloading ids: offset={str(offset)} count={str(remaining_num)} from {server}'), end='')

        # url=server+base_url+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'
        url=api+'search'+library+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'

        # print("->", url)
        try:
            r=requests.get(url, verify=False, timeout=(timeout, 30))
            r.raise_for_status()
        except requests.RequestException as e: 
            print ("Connection issue:", e)
            return
            # pass
        except Exception as e:
            print ("Other issue:", e)
            return
            # pass
        except :
            print ("Wazza !!!!")
            return
        # print("Ids received from:"+str(offset), "to:"+str(offset+remaining_num-1))
        
        # print()
        # print("Downloading metadata from", str(offset+1), "to", str(offset+remaining_num))
        print ('\r {:180.180}'.format(f'Downloading metadata from {str(offset+1)} to {str(offset+remaining_num)}/{total_num} from {server}'), end='')
        books_s=",".join(str(i) for i in r.json()['book_ids'])
        url=api+'books'+library+'?ids='+books_s
        # url=server+base_url+'/books?ids='+books_s
        # print("->", url)
        # print ('\r{:190.190}'.format(f'url= {url} ...'), end='')

        try:
            r=requests.get(url, verify=False, timeout=(60, 60))
            r.raise_for_status()
        except requests.RequestException as e: 
            print ("Connection issue:", e)
            return
            # pass
        except Exception as e:
            print ("Other issue:", e)
            return
            # pass
        except :
            print ("Wazza !!!!")
            return
        # print(len(r.json()), "received")
        print ('\r {:180.180}'.format(f'{len(r.json())} received'), end='')
        
        
        books=[]
        for id, r_book in r.json().items():                
            uuid=r_book['uuid']
            if not uuid:
                print ("No uuid for ebook: ignored")
                continue 


            if r_book['authors']:
                desc= f"({r_book['title']} / {r_book['authors'][0]})"
            else:
                desc= f"({r_book['title']})"

            # print (f'\r--> {range}/{total_num} - {desc}', end='')
            # print (f'\r{server}--> {range}/{total_num} - {desc}', end='')
            print ('\r {:180.180} '.format(f'{range}/{total_num} ({server} : {uuid} --> {desc}'), end='')


            if not force_refresh:
                # print("Checking local metadata:", uuid)
                try:
                    book = load_metadata(dir, uuid)
                except:
                    print("Unable to get metadata from:", uuid)
                    range+=1
                    continue
                if book:
                    print("Metadata already present for:", uuid)
                    range+=1
                    continue

            if not r_book['formats']:
                # print("No format found for {}".format(r_book['uuid']))
                range+=1
                continue

            book={}
            book['uuid']=r_book['uuid']
            book['id']=id
            book['library']=lib

            # book['title']=r_book['title']
            book['title']=unidecode.unidecode(r_book['title'])
            # book['authors']=r_book['authors']

            if r_book['authors']:
                book['authors']=[unidecode.unidecode(s) for s in r_book['authors']]
            # book['desc']=""

            book['desc']=r_book['comments']

            if r_book['series']:
                book['series']=unidecode.unidecode(r_book['series'])
                # book['series']=[unidecode.unidecode(s) for s in r_book['series']]
            s_i=r_book['series_index']
            if (s_i): 
                book['series_index']=int(s_i)

            # book['edition']=0

            book['identifiers']=r_book['identifiers']

            # book['tags']=r_book['tags']
            if r_book['tags']:
                book['tags']=[unidecode.unidecode(s) for s in r_book['tags']]

            book['publisher']=r_book['publisher']
            # book['publisher']=unidecode.unidecode(r_book['publisher'])

            book['pubdate']=r_book['pubdate']

            if not r_book['languages']:
            # if True:
                text=r_book['title']+". "
                if r_book['comments']:
                    text=r_book['comments']                    
                s_language, prob=identifier.classify(text)
                if prob >= 0.85:
                    language =  iso639.to_iso639_2(s_language)
                    book['language']=language
                else:
                    book['language']=''
            else:
                book['language']=iso639.to_iso639_2(r_book['languages'][0])

            if r_book['cover']:
                book['cover']= True
            else:
                book['cover']= False

            book['last_modified']=r_book['last_modified']
            book['timestamp']=r_book['timestamp']

            book['formats']=[]
            formats=r_book['formats']
            for f in formats:                    
                if 'size' in r_book['format_metadata'][f]:
                    size=int(r_book['format_metadata'][f]['size'])
                else:
                    # print()
                    # print(f"Size not found for format '{f}'  uuid={uuid}: skipped")
                    pass
                    #TODO query the size when the function to rebuild the full url is ready
                    #   
                    # print("Trying to get size online: {}".format('url'))
                    # try:
                    #     size=get_file_size(s['url'])
                    # except:
                    #     print("Unable to access size for format '{}' : {} skipped".format(f, uuid))
                    #     continue
                book[f]=(size)
                book['formats'].append(f)

            if not book['formats']:
            # if not c_format:
                # print()
                # print(f"No format found for {book['uuid']} id={book['id']} : skipped")
                range+=1
                # continue


            books.append(book)
            range+=1

        # print()
        print("Saving metadata")
        print ('\r {:180.180}'.format(f'Saving metadata from {server}'), end='')

        try:
            save_books_metadata_from_site(db, books)
            print('\r {:180.180}'.format(f'--> Saved {range-1}/{total_num} ebooks from {server}'), end='')
        except BaseException as err:
            print (err)

        print()
        print()

        # try:
        #     save_metadata(db, books)
        # except:
        #     print("Unable to save book metadata")

        offset=offset+num
    
############################
# Query EBooks in Database #
############################
def query(query_str="", dir="."):
    dbs=[]
    for path in os.listdir(dir):
        db = Database(path)
        # print (db["ebooks"].count)
        # for row in db["site"].rows:
        #     print (f'{row["urls"]}: {db["ebooks"].count}')
        # db["ebooks"].search(query_str)
        # url=db['site'].get(1)['urls'][0]
        url=db['site'].get(1)
        print (url)

        for ebook in db["ebooks"].rows_where(query_str):
            # print (f"{ebook['title']} ({ebook['uuid']})")
            print (ebook)


##################
# Get Statistics #
##################
def get_stats(dir="."):
    dbs=[]
    size=0
    count=0
    for f in os.listdir(dir):
        if not f.endswith(".db"):
            continue
        if f == "index.db":
            continue
        path = Path(dir) / f 
        dbs.append(Database(path))

    for db in dbs:
        for i, ebook in enumerate(db["ebooks"].rows):
            uuid=ebook['uuid']
            title=ebook['title']
            formats=json.loads(ebook['formats'])
            # print(formats)
            for f in formats:
                if f in ebook:
                    if ebook[f]:
                        size+=ebook[f]
                        count+=1
                        # print (f'\r{count} {f} --> {uuid}: {title}', end ='')
                        # print (f'\r{count} : {uuid} --> {f}', end='')
                        print (f'\r{count} formats - ebook : {uuid}', end='')

    print()
    print("Total count of formats:", humanize.intcomma(count)) 
    print("Total size:", hsize(size)) 
    print()

###############################
# Get Temporary Site Database #
###############################
def get_site_db(uuid, dir):
        f_uuid=str(uuid)+".db"
        print(f_uuid)
        path = Path(dir) / str(f_uuid) 
        return Database(path)

################################
# Init Temporary Site Database #
################################
def init_site_db(site, _uuid="", dir="."):
    
    if not _uuid:
        s_uuid=str(uuid.uuid4())
    else:
        s_uuid=str(_uuid)

    f_uuid=s_uuid+".db"
    path = Path(dir) / f_uuid 
    db = Database(path)


    if not "site" in db.table_names():
        s=db["site"]
        s.insert(
            {    
                "uuid": s_uuid,
                "urls": [site],
                "version": "",
                "major": 0,
                "schema_version": 1,
            }
            , pk='uuid'
        )


    if not "ebooks" in db.table_names():
        db["ebooks"].create({
        "uuid": str,
        "id": int,
        "library": str,  #TODO: manage libraries ids as integer to prevent library renam on remote site  
        "title": str,
        "authors": str,
        "series": str,
        "series_index": int,
        # "edition": int, 
        "language": str,
        "desc": str,
        "identifiers": str,
        "tags": str,
        "publisher": str,
        "pubdate": str,
        "last_modified": str,
        "timestamp": str,
        "formats": str,
        "cover": int,
        # "epub": int,
        # "mobi": int,
        # "pdf": int,
        # TODO: add the most common formats to avoid alter tables
        }, pk="uuid")

    if not "libraries" in db.table_names():
        db["libraries"].create({
        "id": int,    
        "names": str
        }, pk="id")


        # db.table("ebooks", pk="id")
        # db.table("ebooks", pk="id", alter=True

    return db

######################
# Format the Get URL #
######################
def get_format_url(db, book, format):
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]
    library=book['library']
    id_=str(book['id'])

    f_url = url+"/get/"+format+"/"+id_+"/"+library
    return f_url
    
########################
# Get Book Description #
########################
def get_desc_url(db, book):
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]

    library=book['library']
    id_=str(book['id'])

    f_urls=[]

    major=  list(db['site'].rows)[0]["major"]

    if major >= 3:
        d_url =url+"#book_id="+id_+"&library_id="+library+"&panel=book_details"
    else:
        d_url =url+"/browse/book/"+id_

    return d_url

###################################
# Save Books Metadata to Database #
###################################
def save_books_metadata_from_site(db, books):
    uuid = list(db['site'].rows)[0]["uuid"]

    # print(uuid)
    
    ebooks_t=db["ebooks"]


    # print([c[1] for c in ebooks_t.columns])
    # for b in books:
    #     print(b['title'])
    #     ebooks_t.insert(b, alter=True)

    # ebooks_t.insert_all(books, alter=True)
    ebooks_t.insert_all(books, alter=True,  pk='uuid', batch_size=1000)
    # print([c[1] for c in ebooks_t.columns])

#################
# Load Metadata #
#################
def load_metadata(dir, uuid):
    pass
######################
# Update Done Status #
######################
def update_done_status(book):
    source=book['source']
    if source['status']!='ignored':
        if set(source['formats'].keys()) == set(book['formats']) & set(source['formats'].keys()):
            book['source']['status']="done"
        else: 
            book['source']['status']="todo"

############################
# Index Site List Sequence #
############################
def index_site_list_seq(file):
    with open(file) as f:
        for s in f.readlines():
            # try: 
            #     index_ebooks(s.rstrip())
            # except:
            #     continue
            index_ebooks(s.rstrip())

###################
# Index Site List #
###################
def index_site_list(file):
    pool = Pool(40)

    with open(file) as f:
        sites = f.readlines()
        sites= [s.rstrip() for s in sites]
        print(sites)
        pool.map(index_ebooks_except, sites)

##########################
# Index Ebooks Exception #
##########################
def index_ebooks_except(site):
    try:
        index_ebooks(site)
    except:
        print("Error on site")

################
# Index Ebooks #
################
def index_ebooks(site, library="", start=0, stop=0, dir=".", num=1000, force_refresh=False):

    #TODO old calibres don't manage libraries.  /ajax/library-info endpoint doesn't exist. It would be better to manage calibre version directly 

    libs=[]
    try:
        libs= get_libs_from_site(site)
    except:
        print("old lib")
        
    _uuid=str(uuid.uuid4())
    
    if libs:
        for lib in libs:
            index_ebooks_from_library(site=site, _uuid=_uuid, library=lib, start=start, stop=stop, dir=dir, num=num, force_refresh=force_refresh)   
    else:
            index_ebooks_from_library(site=site, _uuid=_uuid, start=start, stop=stop, dir=dir, num=num, force_refresh=force_refresh)   

#############################
# Index Ebooks from Library #
#############################
def index_ebooks_from_library(site, _uuid="", library="", start=0, stop=0, dir=".", num=1000, force_refresh=False):
    
    offset= 0 if not start else start-1
    num=min(1000, num)
    server=site.rstrip('/')
    api=server+'/ajax/'
    lib=library
    library= '/'+library if library else library

    timeout=15

    print(f"\nIndexing library: {lib} from server: {server} ")
    url=api+'search'+library+'?num=0'
    print(f"\nGetting ebooks count of library: {lib} from server:{server} ")
    # print(url)
    
    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.RequestException as e: 
        print("Unable to open site:", url)
        return
        # pass
    except Exception as e:
        print ("Other issue:", e)
        return
        # pass
    except :
        print("Wazza !!!!")
        sys.exit(1)
        

    total_num=int(r.json()["total_num"])
    total_num= total_num if not stop else stop
    print()    
    print(f"Total count={total_num} from {server}")
 
    # library=r.json()["base_url"].split('/')[-1]
    # base_url=r.json()["base_url"]

    # cache_db=init_cache_db(dir=dir)
    # _uuid=get_uuid_from_url(cache_db)
    db=init_site_db(site, _uuid=_uuid, dir=dir)
    r_site = (list(db['site'].rows)[0])

    r_site['version']=r.headers['server']
    r_site['major']=int(re.search('calibre.(\d).*', r.headers['server']).group(1))
    db["site"].upsert(r_site, pk='uuid')

    print()

    range=offset+1
    while offset < total_num:
        remaining_num = min(num, total_num - offset)
        # print()
        # print("Downloading ids: offset="+str(offset), "num="+str(remaining_num))
        print ('\r {:180.180}'.format(f'Downloading ids: offset={str(offset)} count={str(remaining_num)} from {server}'), end='')

        # url=server+base_url+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'
        url=api+'search'+library+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'

        # print("->", url)
        try:
            r=requests.get(url, verify=False, timeout=(timeout, 30))
            r.raise_for_status()
        except requests.RequestException as e: 
            print ("Connection issue:", e)
            return
            # pass
        except Exception as e:
            print ("Other issue:", e)
            return
            # pass
        except :
            print ("Wazza !!!!")
            return
        # print("Ids received from:"+str(offset), "to:"+str(offset+remaining_num-1))
        
        # print()
        # print("Downloading metadata from", str(offset+1), "to", str(offset+remaining_num))
        print ('\r {:180.180}'.format(f'Downloading metadata from {str(offset+1)} to {str(offset+remaining_num)}/{total_num} from {server}'), end='')
        books_s=",".join(str(i) for i in r.json()['book_ids'])
        url=api+'books'+library+'?ids='+books_s
        # url=server+base_url+'/books?ids='+books_s
        # print("->", url)
        # print ('\r{:190.190}'.format(f'url= {url} ...'), end='')

        try:
            r=requests.get(url, verify=False, timeout=(60, 60))
            r.raise_for_status()
        except requests.RequestException as e: 
            print ("Connection issue:", e)
            return
            # pass
        except Exception as e:
            print ("Other issue:", e)
            return
            # pass
        except :
            print ("Wazza !!!!")
            return
        # print(len(r.json()), "received")
        print ('\r {:180.180}'.format(f'{len(r.json())} received'), end='')
        
        
        books=[]
        for id, r_book in r.json().items():                
            uuid=r_book['uuid']
            if not uuid:
                print ("No uuid for ebook: ignored")
                continue 


            if r_book['authors']:
                desc= f"({r_book['title']} / {r_book['authors'][0]})"
            else:
                desc= f"({r_book['title']})"

            # print (f'\r--> {range}/{total_num} - {desc}', end='')
            # print (f'\r{server}--> {range}/{total_num} - {desc}', end='')
            print ('\r {:180.180} '.format(f'{range}/{total_num} ({server} : {uuid} --> {desc}'), end='')


            if not force_refresh:
                # print("Checking local metadata:", uuid)
                try:
                    book = load_metadata(dir, uuid)
                except:
                    print("Unable to get metadata from:", uuid)
                    range+=1
                    continue
                if book:
                    print("Metadata already present for:", uuid)
                    range+=1
                    continue

            if not r_book['formats']:
                # print("No format found for {}".format(r_book['uuid']))
                range+=1
                continue

            book={}
            book['uuid']=r_book['uuid']
            book['id']=id
            book['library']=lib

            # book['title']=r_book['title']
            book['title']=unidecode.unidecode(r_book['title'])
            # book['authors']=r_book['authors']

            if r_book['authors']:
                book['authors']=[unidecode.unidecode(s) for s in r_book['authors']]
            # book['desc']=""

            book['desc']=r_book['comments']

            if r_book['series']:
                book['series']=unidecode.unidecode(r_book['series'])
                # book['series']=[unidecode.unidecode(s) for s in r_book['series']]
            s_i=r_book['series_index']
            if (s_i): 
                book['series_index']=int(s_i)

            # book['edition']=0

            book['identifiers']=r_book['identifiers']

            # book['tags']=r_book['tags']
            if r_book['tags']:
                book['tags']=[unidecode.unidecode(s) for s in r_book['tags']]

            book['publisher']=r_book['publisher']
            # book['publisher']=unidecode.unidecode(r_book['publisher'])

            book['pubdate']=r_book['pubdate']

            if not r_book['languages']:
            # if True:
                text=r_book['title']+". "
                if r_book['comments']:
                    text=r_book['comments']                    
                s_language, prob=identifier.classify(text)
                if prob >= 0.85:
                    language =  iso639.to_iso639_2(s_language)
                    book['language']=language
                else:
                    book['language']=''
            else:
                book['language']=iso639.to_iso639_2(r_book['languages'][0])

            if r_book['cover']:
                book['cover']= True
            else:
                book['cover']= False

            book['last_modified']=r_book['last_modified']
            book['timestamp']=r_book['timestamp']

            book['formats']=[]
            formats=r_book['formats']
            for f in formats:                    
                if 'size' in r_book['format_metadata'][f]:
                    size=int(r_book['format_metadata'][f]['size'])
                else:
                    # print()
                    # print(f"Size not found for format '{f}'  uuid={uuid}: skipped")
                    pass
                    #TODO query the size when the function to rebuild the full url is ready
                    #   
                    # print("Trying to get size online: {}".format('url'))
                    # try:
                    #     size=get_file_size(s['url'])
                    # except:
                    #     print("Unable to access size for format '{}' : {} skipped".format(f, uuid))
                    #     continue
                book[f]=(size)
                book['formats'].append(f)

            if not book['formats']:
            # if not c_format:
                # print()
                # print(f"No format found for {book['uuid']} id={book['id']} : skipped")
                range+=1
                # continue


            books.append(book)
            range+=1

        # print()
        print("Saving metadata")
        print ('\r {:180.180}'.format(f'Saving metadata from {server}'), end='')

        try:
            save_books_metadata_from_site(db, books)
            print('\r {:180.180}'.format(f'--> Saved {range-1}/{total_num} ebooks from {server}'), end='')
        except BaseException as err:
            print (err)

        print()
        print()

        # try:
        #     save_metadata(db, books)
        # except:
        #     print("Unable to save book metadata")

        offset=offset+num
        
###########################
# Query Books in Database #
###########################
def query(query_str="", dir="."):
    dbs=[]
    for path in os.listdir(dir):
        db = Database(path)
        # print (db["ebooks"].count)
        # for row in db["site"].rows:
        #     print (f'{row["urls"]}: {db["ebooks"].count}')
        # db["ebooks"].search(query_str)
        # url=db['site'].get(1)['urls'][0]
        url=db['site'].get(1)
        print (url)

        for ebook in db["ebooks"].rows_where(query_str):
            # print (f"{ebook['title']} ({ebook['uuid']})")
            print (ebook)

###########################
# Get Stats on EBook Type #
###########################
def get_stats(dir="."):
    dbs=[]
    size=0
    count=0
    for f in os.listdir(dir):
        if not f.endswith(".db"):
            continue
        if f == "index.db":
            continue
        path = Path(dir) / f 
        dbs.append(Database(path))

    for db in dbs:
        for i, ebook in enumerate(db["ebooks"].rows):
            uuid=ebook['uuid']
            title=ebook['title']
            formats=json.loads(ebook['formats'])
            # print(formats)
            for f in formats:
                if f in ebook:
                    if ebook[f]:
                        size+=ebook[f]
                        count+=1
                        # print (f'\r{count} {f} --> {uuid}: {title}', end ='')
                        # print (f'\r{count} : {uuid} --> {f}', end='')
                        print (f'\r{count} formats - ebook : {uuid}', end='')

    print()
    print("Total count of formats:", humanize.intcomma(count)) 
    print("Total size:", hsize(size)) 
    print()

#######################
# Initialize Index.db #
#######################
def init_index_db(dir="."):
    
    path = Path(dir) / "index.db" 
    
    db_index = Database(path)
    if not "summary" in db_index.table_names():
        db_index["summary"].create({
        "uuid": str,
        "cover": str,
        "title": str,
        # "source": str
        "authors": str,
        "year": str,
        "series": str,
        "language": str,
        "links": str,
        # "desc": str,
        "publisher": str,
        "tags": str,
        "identifiers": str,
        "formats": str
        }
        # )
        , pk="uuid")

        # db_index.table("index", pk="uuid")
        # db_index.table("summary").enable_fts(["title"])
        # db_index["summary"].enable_fts(["title", "authors", "series", "uuid", "language", "identifiers", "tags", "publisher", "formats", "pubdate"])
        db_index["summary"].enable_fts(["title", "authors", "series", "language", "identifiers", "tags", "publisher", "formats", "year"])

    return db_index

###################
# Get Book Covers #
###################
def get_img_url(db, book):
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]

    library=book['library']
    id_=str(book['id'])

    f_urls=[]

    major=  list(db['site'].rows)[0]["major"]

    if major >= 3:
        d_url =url+"/get/thumb/"+id_+"/"+library+ "?sz=600x800"
    else:
        # d_url =url+"/get/thumb/"+id_
        d_url =url+"/get/thumb_90_120/"+id_

    return d_url

################
# Build Index  #
################
def build_index (dir='.', english=True):

    dbs=[]
    for f in os.listdir(dir):
        if not f.endswith(".db"):
            continue
        if f in ("index.db", "sites.db"):
            continue
        p = Path(dir) / f 
        print(f)
        try:
            db = Database(p.resolve())
        except:
            print ("Pb with:", f)
        dbs.append(db)
    
    db_index = init_index_db(dir=dir)
    index_t=db_index["summary"]

    batch_size=10000
    count=0
    summaries=[]

    for db in dbs:
        for i, ebook in enumerate(db["ebooks"].rows):
            if english and (not ebook['language'] or ebook['language'] != "eng"):
                continue
            elif not english and ebook['language'] == "eng":
                continue
            
            
            if ebook['authors']: 
                ebook['authors']=formats=json.loads(ebook['authors'])
            # if ebook['series']:    
            #     ebook['series']=formats=json.loads(ebook['series'])
            if ebook['identifiers']:
                ebook['identifiers']=formats=json.loads(ebook['identifiers'])
            if ebook['tags']: 
                ebook['tags']=formats=json.loads(ebook['tags'])
            ebook['formats']=formats=json.loads(ebook['formats'])
            ebook['links']=""
            summary = {k: v for k, v in ebook.items() if k in ("uuid","title", "authors", "series", "language", "formats", "tags", "publisher", "identifiers")}
            # summary = {k: v for k, v in ebook.items() if k in ("uuid","title", "authors", "series", "identifiers", "language", "tags", "publisher", "formats")}
            summary['title']={'href': get_desc_url(db, ebook), 'label': ebook['title']}

            summary["cover"]= {"img_src": get_img_url(db, ebook), "width": 90}

            formats=[]
            for f in ebook['formats']:
                formats.append({'href': get_format_url(db, ebook, f), 'label': f"{f} ({hsize(ebook[f])})"})
            summary['links']=formats
            
            pubdate=ebook['pubdate'] 
            summary['year']=pubdate[0:4] if pubdate else "" 
            summaries.append(summary)
            # print(summary)
            count+=1
            print (f"\r{count} - ebook handled: {ebook['uuid']}", end='')

            if not count % batch_size:
                # print()
                # print(f"Saving summary by batch: {len(summaries)}")    
                # print(summaries)
                # index_t.upsert_all(summaries, batch_size=1000, pk='uuid')
                # index_t.insert_all(summaries, batch_size=1000, pk='uuid')
                try:
                    index_t.insert_all(summaries, batch_size=batch_size)
                except Exception as e:
                    # dump = [(s['uuid'],s['links']) for s in summaries]
                    # print(dump)
                    print()
                    print("UUID collisions. Probalbly a site duplicate")
                    print(e)
                    print()

                    # index_t.upsert_all(summaries, batch_size=batch_size, pk='uuid')
                    # TODO Some ebooks could be missed. We need to compute the batch list, insert new ebooks and update the site index

                # print("Saved")
                # print()
                summaries=[]

    # print()
    # print("saving summary")    
    # index_t.upsert_all(summaries, batch_size=1000, pk='uuid')
    # index_t.insert_all(summaries, batch_size=1000, pk='uuid')
    try:
        index_t.insert_all(summaries, batch_size=batch_size)
    except:
        print("sqlite3.IntegrityError: UNIQUE constraint failed: summary.uuid")

    # print("summary done")
    # print()
    
    print()
    print("fts")
    index_t.populate_fts(["title", "authors", "series", "identifiers", "language", "tags", "publisher", "formats", "year"])
    print("fts done")

############################
# Search books in Index.db #
############################
def search(query_str, dir=".", links_only=False):
    path = Path(dir) / "index.db" 
    db_index = Database(path)
    # table=db_index["summary"]
    # rows=table.search(query_str)
    # print(rows)
    sites=set()
    ebook_ids=[]
    for ebook in db_index["summary"].search(query_str):
        sites.add(ebook[-1])
        ebook_ids.append((ebook[3], ebook[-1]))
        # print (ebook)
    # print("sites:", sites) 
    # print("ebooks:", ebook_ids) 

    site_dbs={}
    for s in sites:
        f_uuid=s+".db"
        path = Path(dir) / f_uuid 
        site_dbs[s]=Database(path)
        # print(site_dbs[s].tables)
    
    for e in ebook_ids:
        # ebook=site_dbs[e[1]]["ebooks"].get(e[0])
        # print("ebook:", ebook)
        db=site_dbs[e[1]] 
        # ebooks=db.conn.execute("select * from ebooks").fetchone()
        ebook=db.conn.execute(f'select * from ebooks where uuid="{e[0]}"').fetchone()
        url=json.loads(db['site'].get(1)['urls'])[0]
        library=db['site'].get(1)['library']
        formats=json.loads(ebook[14])
        id_=str(ebook[0])

        if not links_only:
            print()
            print("Title:", ebook[2])
            print("Author:", ebook[3])
            print("Serie:", ebook[4])
            print("Formats:", formats)

        for f in formats:
            print(url+"get/"+f+"/"+id_+"/"+library)


#########################
# Index.db to JSON file #
#########################
# https://stackoverflow.com/questions/26692284/how-to-prevent-brokenpipeerror-when-doing-a-flush-in-python
def index_to_json(dir='.'):
    path = Path(dir) / "index.db" 
    db = Database(path)

    # sys.stdout.flush()

    try: 
        for row in db["summary"].rows:
            if row['title']:
                row['title']=json.loads(row['title'])
            if row['authors']:
                row['authors']=json.loads(row['authors'])
#            if row['series']:
#                row['series']=json.loads(row['series']) 
            if row['links']:
                row['links']=json.loads(row['links'])
            if row['tags']:
                row['tags']=json.loads(row['tags'])
            if row['identifiers']:
                row['identifiers']=json.loads(row['identifiers'])
            if row['formats']:
                row['formats']=json.loads(row['formats'])

            json.dump(row, sys.stdout)
            sys.stdout.flush()
            # return
    except BrokenPipeError:
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1) 

######################
# Initialize Diff.db #
######################
def init_diff_db(dir="."):
    
    path = Path(dir) / "diff.db" 
    
    db_diff = Database(path)
    if not "summary" in db_diff.table_names():
        db_diff["summary"].create({
        "uuid": str,
        "title": str,
        # "cover": str,
        # "source": str
        "authors": str,
        "year": str,
        "series": str,
        "language": str,
        "links": str,
        # "desc": str,
        "publisher": str,
        "tags": str,
        "identifiers": str,
        "formats": str,
        "status": str,
        "old_location":str
        }
        # )
        , pk="uuid")

    return db_diff

#######################
# Difference Function #
#######################
def diff(old, new, dir=".", ):
    path = Path(dir) / old 
    db_old = Database(path)

    path = Path(dir) /  new 
    db_new = Database(path)

    path = Path(dir) / "diff.db"
    db_diff =init_diff_db(dir)

    for i, n_book in enumerate(db_new["summary"].rows):
        n_uuid = n_book['uuid']
        print(i, n_uuid)
        try:
            o_book = db_old["summary"].get(n_uuid)
            # print(n_uuid, '=OK')
            o_loc=json.loads(o_book['title'])['href']
            n_loc=json.loads(n_book['title'])['href']
            if o_loc != n_loc :
                print(n_uuid, 'MOVED')
                n_book["status"]="MOVED"
                n_book["old_location"]=o_loc
                n_book.pop ('cover', None)
                db_diff["summary"].insert(n_book, pk='uuid')                  
        except:
            # print(n_uuid, '=NOK')
            n_book.pop ('cover', None)
            n_book["status"]="NEW"
            db_diff["summary"].insert(n_book, pk='uuid')

############################
# Query Calibre by Country #
############################
def calibre_by_country(country):
    page = 1
    apiquery = 'calibre http.status:"200" country:"' + country + '"'+ ',limit=50'
    try:
        results = api.search(apiquery, limit=20)
        filename = country + ".txt"
                
        csvfile = open(filename, 'w')
        for result in results['matches']:
#               Check if the server is insecure (not using HTTPS)
                if 'https' not in result['data']:
                    print(f"Insecure Web-Calibre server found: {result['ip_str']}:{result['port']} in {result['location']['country_name']}")
                    ipaddress = str(result['ip_str'])
                    port = str(result['port'])
                    server_row = 'http://' + ipaddress + ':' + port + '\n'
                    print(server_row)
                    # Add the server to the servers table
                    #site_cursor.execute("INSERT OR IGNORE INTO sites VALUES (url, hostnames,ports, country)", server_row, ipaddress, port, country)
                    csvfile.write(server_row)
    except shodan.APIError as e:
        print ('Error: %s' % e)
    csvfile.close()
    import_urls_from_file(filename)
    return()

#############################################
# Query CalibreWeb server from Server Table #
# And write each server to the server table #
#############################################
def book_search(country):
    import_urls_from_file(country + '.txt')
    return()

############################################
# Output to text file all servers that are #
# marked as online in database.            #
############################################
def output_online_db():
    script = "select url from sites where status='online'"
    df = pd.read_sql(script, site_conn)
    df.to_csv('online.txt', header=False, index=False)
    return()
