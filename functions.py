import requests
from pathlib import Path
from urllib.parse import *
import uuid
import datetime
import gevent
from gevent import monkey
from gevent import Timeout
from gevent.pool import Pool
import ipaddress
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
import unidecode
from requests.adapters import HTTPAdapter
import urllib3
import logging
logging.basicConfig(filename='shodantest.log', encoding='utf-8', level=logging.DEBUG)

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
identifier = LanguageIdentifier.from_modelstring(model, norm_probs=True)
global api
api = shodan.Shodan('sEsxRpsOrBGJANgG1q6qL46xv153NrSV')

global site_conn
data_dir = "./data/"
site_conn = sqlite3.connect(data_dir + "sites.db")
site_cursor = site_conn.cursor()
########################
# Setup Sites Database #
########################
def init_sites_db(dir=data_dir):
    """
    Initializes the sites database.

    Parameters:
    - dir (str): The directory path where the database file will be created. Default is the current directory.

    Returns:
    - db (Database): The initialized database object.

    """
    logging.info("****Setup Sites Database Function****")
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
    """
    Saves a site to the database.

    Parameters:
    - db (Database): The database object to save the site to.
    - site (dict): The site to be saved.

    This function saves a site to the specified database. If the site does not have a 'uuid' key, a new UUID will be generated and assigned to the site before saving it. The site is saved using the 'upsert' method of the database object, with the primary key set to 'uuid'.

    Returns:
    - None

    """
    logging.info("****Save Site Function****")


    # # TODO: Check if the site is not alreday present
    # def save_sites(db, sites):
    #     db["sites"].insert_all(sites, alter=True,  batch_size=100)
    if not 'uuid' in site: 
        site['uuid']=str(uuid.uuid4())    
    print(site)
    logging.info("Site: %s", site)
    db["sites"].upsert(site, pk='uuid')


##########################
# Validate Site and save #
##########################
def check_and_save_site(db, site):
        """
        Check and save a site.

        Args:
            db (database): The database object.
            site (str): The site to be checked and saved.

        Returns:
            None
        """
        logging.info("****Check and Save Function****")

        res= check_calibre_site(site)
        print(res)
        logging.info("Result: %s", res)
        save_site(db, res)

# import pysnooper
# @pysnooper.snoop()

######################
# Check Calibre Site #
######################
def check_calibre_site(site):
    """
    Check the calibre site.

    :param site: A dictionary containing information about the site.
                 It should have the following keys:
                 - "uuid" (str): The UUID of the site.
                 - "url" (str): The URL of the site.
    :return: A dictionary containing the result of the check.
             It has the following keys:
             - "uuid" (str): The UUID of the site.
             - "last_check" (str): The timestamp of the last check.
             - "status" (str): The status of the site, which can be "unauthorized", "down", "online", or "Unknown Error".
             - "last_online" (str): The timestamp of the last online status if the site is online.
             - "error" (int): The HTTP status code if there is an error.
    """

    logging.info("****Check Calibre Site Function****")
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
    logging.info("Getting ebooks count: %s", site['url'])
    print(url)
    logging.info("URL: %s", url)
    
    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.exceptions.HTTPError as e:
        r.status_code
        logging.error("HTTP error: %s", r.status_code)
        ret['error']=r.status_code
        if (r.status_code == 401):
            ret['status']="unauthorized"
            logging.error("HTTP unauthorized")
        else:
            ret['status']="down"
            logging.error("HTTP down")
        return ret
    except requests.RequestException as e: 
        print("Unable to open site:", url)
        logging.error("Unable to open site: %s", url)
        # print (getattr(e, 'message', repr(e)))
        print (e)
        ret['status']="down"
        return ret
    except Exception as e:
        print ("Other issue:", e)
        logging.error("Other issue: %s", e)
        ret['status']='Unknown Error'
        print (e)
        return ret
    except :
        print("Wazza !!!!")
        logging.error("Critical Error: %s", e)
        ret['status']='Critical Error'
        print (e)
        return ret

    try: 
        print("Total count=",r.json()["total_num"])
        logging.info("Total count: %s", r.json()["total_num"])
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
    """
    Retrieve the site UUID from a given URL.

    Args:
        db (Database): The database connection.
        url (str): The URL to extract the site UUID from.

    Returns:
        tuple or None: The row from the 'sites' table if a match is found, None otherwise.
    """
    logging.info("****Get Site UUID from url Function****")
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
    """
    Generates a site map from a given URL.

    Args:
        url (str): The URL to generate the site map from.
    
    Returns:
        dict: A dictionary containing the generated site map. The dictionary has the following keys:
            - 'url' (str): The modified URL with the path removed.
            - 'hostnames' (list): A list containing the hostname extracted from the URL.
            - 'ports' (list): A list containing the port number extracted from the URL as a string.
    """
    logging.info("****Map Site from URL Function****")
    ret={}

    site=urlparse(url)

    print(site)
    site=site._replace(path='')
    logging.info("URL: %s", url)
    ret['url']=urlunparse(site)
    logging.info("Hostnames: %s", site.hostname)
    ret['hostnames']=[site.hostname]
    logging.info("Port: %s", site.port)
    ret['ports']=[str(site.port)]
    return ret

############################################################
# Import the URLS from the temp file and write to Database #
############################################################
def import_urls_from_file(filepath, dir=data_dir):
    """
    Import URLs from a file and add them to a sites database.

    Args:
        filepath (str): The path to the file containing the URLs.
        dir (str, optional): The directory where the sites database is located. Defaults to '.'.

    Returns:
        None
    """

    #TODO skip malformed urls
    #TODO use cache instead
    logging.info("***Importing URLs from file Function*** %s", filepath)
    db=init_sites_db(dir)

    with open(filepath) as f:
        for url in f.readlines():
            url=url.rstrip()
            # url='http://'+url
            if get_site_uuid_from_url(db, url):
                logging.info("'%s' already present", url)
                print(f"'{url}'' already present")
                continue
            print(f"'{url}'' added")
            logging.info("'%s' added", url)
            save_site(db, map_site_from_url(url))
    
###################################
# Get list of libraries from site #
###################################
def get_libs_from_site(site):
    """
    Retrieves libraries from a specified website.

    Args:
        site (str): The URL of the website to retrieve libraries from.

    Returns:
        list[str]: A list of libraries retrieved from the website.

    Raises:
        requests.RequestException: If there is an issue making the request to the website.
    """
    logging.info("****Get Libs from site Function****")
    server=site.rstrip('/')
    api=server+'/ajax/'
    timeout=30
    
    print()
    print("Server:", server)
    logging.info("Server: %s", server)
    url=api+'library-info'

    print()
    print("Getting libraries from", server)
    logging.info("Getting libraries from: %s", server)
    # print(url)

    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.RequestException as e: 
        print("Unable to open site:", url)
        logging.error("Unable to open site: %s", url)
        # return
    except Exception as e:
        logging.error("Other issue: %s", e)
        print ("Other issue:", e)
        return
        # pass

    libraries = r.json()["library_map"].keys()
    logging.info("Libraries: %s", libraries)
    print("Libraries:", ", ".join(libraries))
    return libraries

###################################
# Check the list of sites in file #
###################################
def check_calibre_list(dir=data_dir):    
    """
    Generates a comment for the given function body in a markdown code block with the correct language syntax.

    Parameters:
        dir (str): The directory to search for the sites database. Defaults to the current directory.

    Returns:
        None
    """
    logging.info("****Check Calibre List Function****")
    db=init_sites_db(dir)
    sites=[]
    for row in db["sites"].rows:
        logging.info("Queueing: %s", row['url'])
        print(f"Queueing:{row['url']}")
        sites.append(row)
    print(sites)
    pool = Pool(100)
    pool.map(lambda s: check_and_save_site (db, s), sites)

#################
# Get site UUID #
#################
# example of a fts search sqlite-utils index.db "select * from summary_fts where summary_fts  match 'title:fre*'"
def get_site_db(uuid, data_dir):
        """
        Retrieves the site database based on the given UUID and directory.

        :param uuid: The UUID of the site.
        :type uuid: int or str
        :param dir: The directory where the site database is located.
        :type dir: str
        :return: The site database.
        :rtype: Database
        """
        logging.info("****Get Site DB Function****")
        f_uuid=str(uuid)+".db"
        logging.info(f_uuid)
        print(f_uuid)
        path = Path(dir) / str(f_uuid) 
        return Database(path)

############################
# Initialize Site Database #
############################
def init_site_db(site, _uuid="", dir=data_dir):
    """
    Initializes a site database.

    Parameters:
        site (str): The URL of the site.
        _uuid (str, optional): The UUID for the database. Defaults to an empty string.
        dir (str, optional): The directory where the database will be created. Defaults to ".".

    Returns:
        Database: The initialized database.

    """
    logging.info("****Init Site DB Function****")
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
        "publisher": str, #Index Ebooks From Library Function
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
    """
    Generate the URL for a specific book format.

    Args:
        db (dict): The database containing the site information.
        book (dict): The book information.
        format (str): The desired book format.

    Returns:
        str: The URL for the specified book format.
    """
    logging.info("****Get Format URL Function****")
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]
    library=book['library']
    id_=str(book['id'])

    f_url = url+"/get/"+format+"/"+id_+"/"+library
    return f_url
    
############################
# Get Library Version Info #
############################
def get_desc_url(db, book):
    """
    Generate the URL for the book description.

    Parameters:
        db (dict): The database containing the site information.
        book (dict): The book object.

    Returns:
        str: The URL for the book description.
    """
    logging.info("****Get Desc URL Function****")
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
    """
    Saves the metadata of books from a website into the database.

    Parameters:
    - db (dict): The database object.
    - books (list): A list of dictionaries containing the metadata of the books.

    Returns:
    - None
    """
    logging.info("****Save Books Metadata From Site Function****")
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

##########################################
# Update Status when book details loaded #
##########################################
def update_done_status(book):
    """
    Update the status of a book based on its source.

    Args:
        book (dict): The book object containing the source information.

    Returns:
        None: This function does not return anything.
    """
    logging.info("****Update Done Status Function****")
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
    """
    Reads a file line by line and calls the index_ebooks function on each line.

    Parameters:
        file (str): The path to the file to be read.

    Returns:
        None
    """
    logging.info("****Index Site List Sequence Function****")
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
    """
    Indexes a list of sites in parallel using a pool of worker processes.

    Args:
        file (str): The path to the file containing the list of sites.

    Returns:
        None
    """
    logging.info("****Index Site List Function****")
    pool = Pool(40)

    with open(file) as f:
        sites = f.readlines()
        sites= [s.rstrip() for s in sites]
        logging.info("Sites: "+str(sites))
        print(sites)
        pool.map(index_ebooks_except, sites)

##########################
# Index ebooks Exception #
##########################
def index_ebooks_except(site):
    """
    Indexes ebooks for a given site, except when an error occurs.

    Args:
        site (str): The site to index ebooks for.

    Returns:
        None
    """
    logging.info("****Index ebooks Exception Function****")
    try:
        index_ebooks(site)
    except:
        print("Error on site")
        logging.error("Error on site: "+site)

################
# Index Ebooks #
################
def index_ebooks(site, library="", start=0, stop=0, dir=data_dir, num=1000, force_refresh=False):
    """
    Retrieves ebooks from a site and indexes them into a library.

    Args:
        site (str): The site from which to retrieve ebooks.
        library (str, optional): The library in which to index the ebooks. Defaults to "".
        start (int, optional): The starting index of ebooks to retrieve. Defaults to 0.
        stop (int, optional): The ending index of ebooks to retrieve. Defaults to 0.
        dir (str, optional): The directory in which to store the ebooks. Defaults to ".".
        num (int, optional): The number of ebooks to retrieve. Defaults to 1000.
        force_refresh (bool, optional): Whether to force a refresh of the ebooks. Defaults to False.

    Returns:
        None
    """

    #TODO old calibres don't manage libraries.  /ajax/library-info endpoint doesn't exist. It would be better to manage calibre version directly 
    logging.info("****Index Ebooks Function****")
    libs=[]
    try:
        libs= get_libs_from_site(site)
    except:
        print("old lib")
        logging.error("Error on site (Old Lib): "+site)
        
    _uuid=str(uuid.uuid4())
    
    if libs:
        for lib in libs:
            index_ebooks_from_library(site=site, _uuid=_uuid, library=lib, start=start, stop=stop, dir=data_dir, num=num, force_refresh=force_refresh)   
    else:
            index_ebooks_from_library(site=site, _uuid=_uuid, start=start, stop=stop, dir=data_dir, num=num, force_refresh=force_refresh)   

#############################
# Index Ebooks from Library #
#############################
def index_ebooks_from_library(site, _uuid="", library="", start=0, stop=0, dir=data_dir, num=1000, force_refresh=False):
    """
    Index ebooks from a library on a site.

    Args:
        site (str): The site to index the library from.
        _uuid (str, optional): The UUID of the library. Defaults to "".
        library (str, optional): The library name. Defaults to "".
        start (int, optional): The starting index for indexing. Defaults to 0.
        stop (int, optional): The stopping index for indexing. Defaults to 0.
        dir (str, optional): The directory to save the indexed ebooks. Defaults to ".".
        num (int, optional): The number of ebooks to index at a time. Defaults to 1000.
        force_refresh (bool, optional): Whether to force refresh the metadata. Defaults to False.

    Returns:
        None
    """
    logging.info("****Index Ebooks from Library Function****")
    offset= 0 if not start else start-1
    num=min(1000, num)
    server=site.rstrip('/')
    api=server+'/ajax/'
    lib=library
    library= '/'+library if library else library

    timeout=15

    print(f"\nIndexing library: {lib} from server: {server} ")
    logging.info(f"Indexing library: {lib} from server: {server} ")
    url=api+'search'+library+'?num=0'
    print(f"\nGetting ebooks count of library: {lib} from server:{server} ")
    logging.info(f"Getting ebooks count of library: {lib} from server:{server} ")
    # print(url)
    
    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.RequestException as e: 
        print("Unable to open site:", url)
        logging.error("Unable to open site: "+url)
        return
        # pass
    except Exception as e:
        print ("Other issue:", e)
        logging.error("Other issue: "+str(e))
        return
        # pass
    except :
        print("Wazza !!!!")
        sys.exit(1)
        

    total_num=int(r.json()["total_num"])
    total_num= total_num if not stop else stop
    print()    
    print(f"Total count={total_num} from {server}")
    logging.info(f"Total count={total_num} from {server}")
    # library=r.json()["base_url"].split('/')[-1]
    # base_url=r.json()["base_url"]

    # cache_db=init_cache_db(dir=dir)
    # _uuid=get_uuid_from_url(cache_db)
    db=init_site_db(site, _uuid=_uuid, dir=data_dir)
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
        logging.info(f"Downloading ids: offset={str(offset)} count={str(remaining_num)} from {server}")

        # url=server+base_url+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'
        url=api+'search'+library+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'

        # print("->", url)
        try:
            r=requests.get(url, verify=False, timeout=(timeout, 30))
            r.raise_for_status()
        except requests.RequestException as e: 
            print ("Connection issue:", e)
            logging.error("Connection issue: "+str(e))
            return
            # pass
        except Exception as e:
            print ("Other issue:", e)
            logging.error("Other issue: "+str(e))
            return
            # pass
        except :
            print ("Wazza !!!!")
            logging.error("Wazza !!!!")
            return
        # print("Ids received from:"+str(offset), "to:"+str(offset+remaining_num-1))
        
        # print()
        # print("Downloading metadata from", str(offset+1), "to", str(offset+remaining_num))
        print ('\r {:180.180}'.format(f'Downloading metadata from {str(offset+1)} to {str(offset+remaining_num)}/{total_num} from {server}'), end='')
        logging.info("Downloading metadata from "+str(offset+1)+" to "+str(offset+remaining_num))
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
            logging.error("Connection issue: "+str(e))
            return
            # pass
        except Exception as e:
            print ("Other issue:", e)
            logging.error("Other issue: "+str(e))
            return
            # pass
        except :
            print ("Wazza !!!!")
            logging.error("Wazza !!!!")
            return
        # print(len(r.json()), "received")
        print ('\r {:180.180}'.format(f'{len(r.json())} received'), end='')
        logging.info(f"{len(r.json())} received")
        
        books=[]
        for id, r_book in r.json().items():                
            uuid=r_book['uuid']
            if not uuid:
                print ("No uuid for ebook: ignored")
                logging.info("No uuid for ebook: ignored")
                continue 


            if r_book['authors']:
                desc= f"({r_book['title']} / {r_book['authors'][0]})"
            else:
                desc= f"({r_book['title']})"

            # print (f'\r--> {range}/{total_num} - {desc}', end='')
            # print (f'\r{server}--> {range}/{total_num} - {desc}', end='')
            print ('\r {:180.180} '.format(f'{range}/{total_num} ({server} : {uuid} --> {desc}'), end='')
            logging.info(f"{range}/{total_num} ({server} : {uuid} --> {desc}")

            if not force_refresh:
                # print("Checking local metadata:", uuid)
                try:
                    book = load_metadata(data_dir, uuid)
                except:
                    print("Unable to get metadata from:", uuid)
                    logging.error("Unable to get metadata from: "+str(uuid))
                    range+=1
                    continue
                if book:
                    print("Metadata already present for:", uuid)
                    logging.error("Metadata already present for: "+str(uuid))
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
        logging.info("Saving metadata from %s", server)
        try:
            save_books_metadata_from_site(db, books)
            print('\r {:180.180}'.format(f'--> Saved {range-1}/{total_num} ebooks from {server}'), end='')
            logging.info("Saved %s/%s ebooks from %s", range-1, total_num, server)
        except BaseException as err:
            print (err)
            logging.error(err)
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
def query(query_str="", dir=data_dir):
    """
    Generates a function comment for the given function body in a markdown code block with the correct language syntax.

    Parameters:
    - query_str (str): The query string to be used in the function.
    - dir (str): The directory to search for files. Default is the current directory.

    Returns:
    - None
    """
    logging.info("Querying database: %s", query_str)
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
        logging.info("Querying %s", url)

        for ebook in db["ebooks"].rows_where(query_str):
            # print (f"{ebook['title']} ({ebook['uuid']})")
            print (ebook)
            logging.info("%s (%s)", ebook['title'], ebook['uuid'])

##################
# Get Statistics #
##################
def get_stats(dir=data_dir):
    """
    Retrieves statistics about the ebooks in a given directory.

    Parameters:
    - dir (str): The directory path to search for ebooks. Defaults to the current directory.

    Returns:
    None
    """
    logging.info("*** get_stats ***")
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
                        logging.info("%s : %s", uuid, f)

    print()
    print("Total count of formats:", humanize.intcomma(count)) 
    logging.info("Total count of formats: %s", humanize.intcomma(count))
    print("Total size:", hsize(size)) 
    logging.info("Total size: %s", hsize(size))
    print()

###############################
# Get Temporary Site Database #
###############################
def get_site_db(uuid, data_dir):
        """
        Generate a function comment for the given function body.

        Args:
            uuid (int): The unique identifier for the site database.
            dir (str): The directory where the site database is located.

        Returns:
            Database: The site database corresponding to the given UUID.
        """
        logging.info("*** get_site_db ***")
        f_uuid=str(uuid)+".db"
        print(f_uuid)
        logging.info(f_uuid)
        path = Path(dir) / str(f_uuid) 
        return Database(path)

################################
# Init Temporary Site Database #
################################
def init_site_db(site, _uuid="", dir=data_dir):
    """
    Initializes a site database.

    Args:
        site (str): The site URL.
        _uuid (str, optional): The UUID for the database. Defaults to an empty string.
        dir (str, optional): The directory where the database file will be created. Defaults to ".".

    Returns:
        Database: The initialized site database.
    """
    logging.info("*** init_site_db ***")
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
    """
    Generates the URL for a specific format of a book.

    Parameters:
        db (dict): The database containing book information.
        book (dict): The book for which to generate the URL.
        format (str): The desired format of the book.

    Returns:
        str: The URL for the specified format of the book.
    """
    logging.info("****Get Format URL Function****")
    url = json.loads(list(db['site'].rows)[0]["urls"])[0]
    library=book['library']
    id_=str(book['id'])

    f_url = url+"/get/"+format+"/"+id_+"/"+library
    return f_url
    
########################
# Get Book Description #
########################
def get_desc_url(db, book):
    """
    Retrieves the description URL for a given book from the database.

    Parameters:
        db (Database): The database object containing the book information.
        book (dict): The book object containing the book details.

    Returns:
        str: The description URL for the given book.
    """
    logging.info("****Get Description URL Function****")
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
    """
    Saves the metadata of books from a website into the database.

    Args:
        db (Database): The database object where the metadata will be saved.
        books (List[dict]): A list of dictionaries representing the metadata of the books.

    Returns:
        None
    """
    logging.info("****Save Books Metadata Function****")
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
def load_metadata(data_dir, uuid):
    """
    Load metadata from a directory using the specified UUID.

    Args:
        dir (str): The directory from which to load the metadata.
        uuid (str): The UUID of the metadata to load.

    Returns:
        None

    Raises:
        None
    """
    logging.info("****Load Metadata Function****")
    pass
######################
# Update Done Status #
######################
def update_done_status(book):
    """
    Updates the 'status' field of the given book's source based on the availability of formats.

    Args:
        book (dict): The book object containing the source and formats information.

    Returns:
        None
    """
    logging.info("****Update Done Status Function****")
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
    """
    Indexes a list of site sequences from a file.

    Args:
        file (str): The path to the file containing the site sequences.

    Returns:
        None
    """
    logging.info("****Index Site List Sequence Function****")
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
    """
    Indexes a list of sites from a given file.

    Args:
        file (str): The path to the file containing the list of sites.

    Returns:
        None
    """
    logging.info("****Index Site List Function****")
    pool = Pool(40)

    with open(file) as f:
        sites = f.readlines()
        sites= [s.rstrip() for s in sites]
        print(sites)
        logging.info("Sites: "+str(sites))
        pool.map(index_ebooks_except, sites)

##########################
# Index Ebooks Exception #
##########################
def index_ebooks_except(site):
    """
    Indexes ebooks on the given site, excluding any errors that occur during indexing.

    :param site: The site to index ebooks from.
    :type site: str

    :return: None
    """
    logging.info("****Index Ebooks Exception Function****")
    try:
        index_ebooks(site)
    except:
        print("Error on site")
        logging.error("Error on site: %s", site)

################
# Index Ebooks #
################
def index_ebooks(site, library="", start=0, stop=0, dir=data_dir, num=1000, force_refresh=False):
    """
    Generates a function comment for the given function body.

    Args:
        site (str): The site to index ebooks from.
        library (str, optional): The library to index ebooks from. Defaults to "".
        start (int, optional): The starting index. Defaults to 0.
        stop (int, optional): The stopping index. Defaults to 0.
        dir (str, optional): The directory to save the indexed ebooks. Defaults to ".".
        num (int, optional): The number of ebooks to index. Defaults to 1000.
        force_refresh (bool, optional): Whether to force a refresh of the indexed ebooks. Defaults to False.
    
    Returns:
        None
    """
    logging.info("****Index Ebooks Function****")
    #TODO old calibres don't manage libraries.  /ajax/library-info endpoint doesn't exist. It would be better to manage calibre version directly 

    libs=[]
    try:
        libs= get_libs_from_site(site)
    except:
        print("old lib")
        logging.error("old lib: %s", site)
        
    _uuid=str(uuid.uuid4())
    
    if libs:
        for lib in libs:
            index_ebooks_from_library(site=site, _uuid=_uuid, library=lib, start=start, stop=stop, dir=data_dir, num=num, force_refresh=force_refresh)   
    else:
            index_ebooks_from_library(site=site, _uuid=_uuid, start=start, stop=stop, dir=data_dir, num=num, force_refresh=force_refresh)   

#############################
# Index Ebooks from Library #
#############################
def index_ebooks_from_library(site, _uuid="", library="", start=0, stop=0, dir=data_dir, num=1000, force_refresh=False):
    """
    Indexes ebooks from a library on a specific site.
    
    Parameters:
    - site: The URL of the site.
    - _uuid: The UUID of the library (optional).
    - library: The name of the library (optional).
    - start: The starting index of the ebooks to index (optional).
    - stop: The stopping index of the ebooks to index (optional).
    - dir: The directory to save the indexed ebooks (optional).
    - num: The maximum number of ebooks to index (optional).
    - force_refresh: Whether to force a refresh of the indexed ebooks (optional).
    
    Returns:
    None
    """
    logging.info("****Index Ebooks From Library Function****")
    offset= 0 if not start else start-1
    num=min(1000, num)
    server=site.rstrip('/')
    api=server+'/ajax/'
    lib=library
    library= '/'+library if library else library

    timeout=15

    print(f"\nIndexing library: {lib} from server: {server} ")
    logging.info("Indexing library: %s from server: %s ", lib, server)
    url=api+'search'+library+'?num=0'
    print(f"\nGetting ebooks count of library: {lib} from server:{server} ")
    logging.info("Getting ebooks count of library: %s from server: %s ", lib, server)
    # print(url)
    
    try:
        r=requests.get(url, verify=False, timeout=(timeout, 30))
        r.raise_for_status()
    except requests.RequestException as e: 
        print("Unable to open site:", url)
        logging.info("Unable to open site: %s", url)
        return
        # pass
    except Exception as e:
        print ("Other issue:", e)
        logging.error("Other issue: %s", e)
        return
    except :
        print("Wazza !!!!")
        logging.error("Wazza !!!!")
        sys.exit(1)
        

    total_num=int(r.json()["total_num"])
    total_num= total_num if not stop else stop
    print()    
    print(f"Total count={total_num} from {server}")
    logging.info("Total count=%s from %s", total_num, server)

    db=init_site_db(site, _uuid=_uuid, dir=dir)
    r_site = (list(db['site'].rows)[0])

    r_site['version']=r.headers['server']
    r_site['major']=int(re.search('calibre.(\d).*', r.headers['server']).group(1))
    db["site"].upsert(r_site, pk='uuid')

    print()

    range=offset+1
    while offset < total_num:
        remaining_num = min(num, total_num - offset)
        print ('\r {:180.180}'.format(f'Downloading ids: offset={str(offset)} count={str(remaining_num)} from {server}'), end='')
        logging.info("Downloading ids: offset=%s count=%s from %s", str(offset), str(remaining_num), server)
        # url=server+base_url+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'
        url=api+'search'+library+'?num='+str(remaining_num)+'&offset='+str(offset)+'&sort=timestamp&sort_order=desc'

        # print("->", url)
        try:
            r=requests.get(url, verify=False, timeout=(timeout, 30))
            r.raise_for_status()
        except requests.RequestException as e: 
            print ("Connection issue:", e)
            logging.error("Connection issue: %s", e)
            return
            # pass
        except Exception as e:
            print ("Other issue:", e)
            logging.error("Other issue: %s", e)
            return
            # pass
        except :
            print ("Wazza !!!!")
            logging.error("Wazza !!!!")
            return

        print ('\r {:180.180}'.format(f'Downloading metadata from {str(offset+1)} to {str(offset+remaining_num)}/{total_num} from {server}'), end='')
        logging.info("Downloading metadata from %s to %s/%s from %s", str(offset+1), str(offset+remaining_num), total_num, server)
        books_s=",".join(str(i) for i in r.json()['book_ids'])
        url=api+'books'+library+'?ids='+books_s

        try:
            r=requests.get(url, verify=False, timeout=(60, 60))
            r.raise_for_status()
        except requests.RequestException as e: 
            print ("Connection issue:", e)
            logging.error("Connection issue: %s", e)
            return
        except Exception as e:
            print ("Other issue:", e)
            logging.error("Other issue: %s", e)
            return
        except :
            print ("Wazza !!!!")
            logging.error("Wazza !!!!")
            return
        print ('\r {:180.180}'.format(f'{len(r.json())} received'), end='')
        logging.info("%s received", len(r.json()))        
        
        books=[]
        for id, r_book in r.json().items():                
            uuid=r_book['uuid']
            if not uuid:
                print ("No uuid for ebook: ignored")
                logging.info("No uuid for ebook: ignored")
                continue 


            if r_book['authors']:
                desc= f"({r_book['title']} / {r_book['authors'][0]})"
            else:
                desc= f"({r_book['title']})"

            print ('\r {:180.180} '.format(f'{range}/{total_num} ({server} : {uuid} --> {desc}'), end='')
            logging.info("%s/%s (%s : %s --> %s)", range, total_num, server, uuid, desc)

            if not force_refresh:
                try:
                    book = load_metadata(dir, uuid)
                except:
                    print("Unable to get metadata from:", uuid)
                    logging.error("Unable to get metadata from: %s", uuid)
                    range+=1
                    continue
                if book:
                    print("Metadata already present for:", uuid)
                    logging.error("Metadata already present for: %s", uuid)
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
        logging.info("Saving metadata")
        print ('\r {:180.180}'.format(f'Saving metadata from {server}'), end='')
        logging.info("Saving metadata from %s", server)
        try:
            save_books_metadata_from_site(db, books)
            print('\r {:180.180}'.format(f'--> Saved {range-1}/{total_num} ebooks from {server}'), end='')
            logging.info("--> Saved %s/%s ebooks from %s", range-1, total_num, server)
        except BaseException as err:
            print (err)
            logging.error(err)

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
def query(query_str="", dir=data_dir):
    """
    This function takes in an optional query string and directory path and performs a query on a list of databases located in the specified directory.
    
    Parameters:
        query_str (str): The query string to be used for searching the databases. Default is an empty string.
        dir (str): The directory path where the databases are located. Default is the current directory.
    
    Returns:
        None
    """
    logging.info("****Query Function****")
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
        logging.info("Querying %s",url)

        for ebook in db["ebooks"].rows_where(query_str):
            # print (f"{ebook['title']} ({ebook['uuid']})")
            print (ebook)
            logging.info("Found %s (%s)", ebook['title'], ebook['uuid'])

###########################
# Get Stats on EBook Type #
###########################
def get_stats(dir=data_dir):
    """
    Retrieves statistics about the ebooks in the specified directory.

    Parameters:
        dir (str): The directory to search for ebooks. Defaults to the current directory.

    Returns:
        None
    """
    logging.info("****Get Stats Function****")
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
                        logging.info("Found %s (%s)", title, uuid)

    print()
    print("Total count of formats:", humanize.intcomma(count)) 
    logging.info("Total count of formats: %s", humanize.intcomma(count))
    print("Total size:", hsize(size)) 
    logging.info("Total size: %s", hsize(size))
    print()

#######################
# Initialize Index.db #
#######################
def init_index_db(dir=data_dir):
    """
    Initializes an index database in the specified directory.
    
    Args:
        dir (str): The directory where the index database should be created. Defaults to the current directory.
        
    Returns:
        Database: The initialized index database.
    """
    
    logging.info("****Initialize Index Function****")
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
    """
    Generates the URL of an image based on the database and book information.

    Parameters:
        db (dict): The database containing the site information.
        book (dict): The book information.

    Returns:
        str: The URL of the image.
    """
    logging.info("****Get Img URL Function****")
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
def build_index(dir=data_dir):
    """
    Builds an index for English ebooks in the given directory.

    Args:
        dir (str): The directory to search for ebook databases. Defaults to the current directory.
        english (bool): Flag to indicate whether to include only English ebooks in the index. Defaults to True.

    Returns:
        None
    """
    dir=data_dir
    logging.info("****Build Index Function****")
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
            logging.info("Pb with:", f)
        dbs.append(db)
    
    db_index = init_index_db(dir=dir)
    index_t=db_index["summary"]

    batch_size=10000
    count=0
    summaries=[]

    for db in dbs:
        for i, ebook in enumerate(db["ebooks"].rows):
#            if english and (not ebook['language'] or ebook['language'] != "eng"):
#                continue
#            elif not english and ebook['language'] == "eng":
#                continue

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
            logging.info("Summary: %s", summary)
            count+=1
            print (f"\r{count} - ebook handled: {ebook['uuid']}", end='')
            logging.info(f"\r{count} - ebook handled: {ebook['uuid']}")
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
                    logging.error("UUID collisions. Probalbly a site duplicate")
                    print(e)
                    logging.error("Error: %s", e)
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
        logging.error("sqlite3.IntegrityError: UNIQUE constraint failed: summary.uuid")
    # print("summary done")
    # print()
    
    print()
    print("fts")
    logging.error("fts")
    index_t.populate_fts(["title", "authors", "series", "identifiers", "language", "tags", "publisher", "formats", "year"])
    print("fts done")
    logging.error("fts done")

############################
# Search books in Index.db #
############################
def search(query_str, dir=data_dir, links_only=False):
    """
    Search for ebooks in the specified directory using a query string.
    
    Args:
        query_str (str): The query string to search for.
        dir (str, optional): The directory to search in. Defaults to ".".
        links_only (bool, optional): Whether to only print the download links. Defaults to False.
    
    Returns:
        None
    """
    logging.info("****Search Function****")
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
            logging.info("Title: %s", ebook[2])
            print("Author:", ebook[3])
            logging.info("Author: %s", ebook[3])
            print("Serie:", ebook[4])
            logging.info("Serie: %s", ebook[4])
            print("Formats:", formats)
            logging.info("Formats: %s", formats)

        for f in formats:
            print(url+"get/"+f+"/"+id_+"/"+library)
            logging.info(url+"get/"+f+"/"+id_+"/"+library)
            
#########################
# Index.db to JSON file #
#########################
# https://stackoverflow.com/questions/26692284/how-to-prevent-brokenpipeerror-when-doing-a-flush-in-python
def index_to_json(dir=data_dir):
    """
    Converts the index database to a JSON file.

    Args:
        dir (str): The directory containing the index database file. Defaults to the current directory.

    Returns:
        None
    """
    logging.info("****Index to JSON Function****")
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
        logging.error("BrokenPipeError")
        devnull = os.open(os.devnull, os.O_WRONLY)
        os.dup2(devnull, sys.stdout.fileno())
        sys.exit(1) 

######################
# Initialize Diff.db #
######################
def init_diff_db(dir=data_dir):
    """
    Initializes a new database for storing diff information.

    Args:
        dir (str): The directory where the database file should be created. Defaults to the current directory.

    Returns:
        Database: The initialized diff database.

    Raises:
        None
    """
    logging.info("****Initialize Diff Function****")
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
def diff(old, new, dir=data_dir, ):
    """
    Generate the function comment for the given function body in a markdown code block with the correct language syntax.

    :param old: The old value.
    :param new: The new value.
    :param dir: The directory where the files are located. Defaults to ".".
    """
    logging.info("****Diff Function****")
    path = Path(dir) / old 
    db_old = Database(path)

    path = Path(dir) /  new 
    db_new = Database(path)

    path = Path(dir) / "diff.db"
    db_diff =init_diff_db(dir)

    for i, n_book in enumerate(db_new["summary"].rows):
        n_uuid = n_book['uuid']
        print(i, n_uuid)
        logging.info(i, n_uuid)
        try:
            o_book = db_old["summary"].get(n_uuid)
            # print(n_uuid, '=OK')
            o_loc=json.loads(o_book['title'])['href']
            n_loc=json.loads(n_book['title'])['href']
            if o_loc != n_loc :
                print(n_uuid, 'MOVED')
                logging.info(n_uuid, 'MOVED')
                n_book["status"]="MOVED"
                n_book["old_location"]=o_loc
                n_book.pop ('cover', None)
                db_diff["summary"].insert(n_book, pk='uuid')                  
        except:
            # print(n_uuid, '=NOK')
            n_book.pop ('cover', None)
            logging.error(n_uuid, '=NOK')
            n_book["status"]="NEW"
            logging.error(n_uuid, 'NEW')
            db_diff["summary"].insert(n_book, pk='uuid')
            logging.error(n_uuid, 'inserted')


############################
# Query Calibre by Country #
############################
def calibre_by_country(country):
    """
    Generates a list of web-Calibre servers by country and saves them to a file.

    Args:
        country (str): The country for which to generate the list of web-Calibre servers.

    Returns:
        None
    """
    logging.info("****Calibre by Country Function****")
    page = 1
    apiquery = 'calibre http.status:"200" country:"' + country + '"'+ ',limit=50'
    try:
        print('apiquery= ', apiquery)
        results = api.search(apiquery, limit=40)
        filename = "./data/" + country + ".txt"
        csvfile = open(filename, 'w')
        for result in results['matches']:
#               Check if the server is insecure (not using HTTPS)
                    if 'https' not in result['data']:
                        print(f"Insecure Web-Calibre server found: {result['ip_str']}:{result['port']} in {result['location']['country_name']}")
                        logging.info(f"Insecure Web-Calibre server found: {result['ip_str']}:{result['port']} in {result['location']['country_name']}")
                        ipaddress = str(result['ip_str'])
                        port = str(result['port'])
                        server_row = 'http://' + ipaddress + ':' + port + '\n'
                        print(server_row)
                        logging.info(server_row)
                        # Add the server to the servers table
                        #site_cursor.execute("INSERT OR IGNORE INTO sites VALUES (url, hostnames,ports, country)", server_row, ipaddress, port, country)
                        csvfile.write(server_row)
    except shodan.APIError as e:
        print ('Error: %s' % e)
        logging.error('Error: %s' % e)
    csvfile.close()
    import_urls_from_file(filename)
    return()

#############################################
# Query CalibreWeb server from Server Table #
# And write each server to the server table #
#############################################
def book_search(country):
    """
    Function to search for books based on the country.

    Args:
        country (str): The name of the country to search for books.

    Returns:
        None
    """
    print("Starting Book Search for Country: ", country)
    logging.info("Starting Book Search for Country:", country)
    logging.info("****Book Search Function****")
    import_urls_from_file(country + '.txt')
    print("Ending Book Search for: ", country)
    logging.info("Ending Book Search for: ", country)
    return()

############################################
# Output to text file all servers that are #
# marked as online in database.            #
############################################
def output_online_db():
    """
    Retrieves the URLs of all online sites from the database and saves them to a file.

    Parameters:
    None

    Returns:
    None
    """
    logging.info("****Output Online DB Function****")
    script = "select url from sites where status='online'"
    print("site_conn = ", site_conn)
    df = pd.read_sql(script, site_conn)
    df.to_csv(data_dir + 'online.txt', header=False, index=False)
    return()
