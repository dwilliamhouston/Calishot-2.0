import requests
import sys
import sqlite3
import csv
import pathlib
import jq
import json
import os
import subprocess
import shodan
from functions import import_urls_from_file, check_calibre_list, index_site_list, index_site_list, get_stats, index_site_list_seq, import_urls_from_file, check_calibre_list, check_calibre_site, build_index, index_to_json, diff, calibre_by_country, book_search, output_online_db

#####################
# Flags for testing #
#####################
run_search_by_country = True
run_book_search = True
run_check_calibre_list = True
run_output_online_db = True
run_index_site_list = True
run_index_site_list_seq = True
run_build_index = True
run_get_stats = True
run_index_to_json = True

####################################################
# Call search_by_country Function for each Country #
####################################################
run_search_by_country = False
if run_search_by_country:
    print ("Running search_by_country...")
    calibre_by_country('AU')
    calibre_by_country('NZ')
    calibre_by_country('IE')
    calibre_by_country('UK')
    calibre_by_country('CA')
    calibre_by_country('NL')
    calibre_by_country('DE')
    calibre_by_country('US')
    
##############################################
# Call book_search Function for each Country #
##############################################
run_book_search = False
if run_book_search:
    print ("Running run_book_search...")
    book_search('AU')
    book_search('NZ')
    book_search('IE')
    book_search('UK')
    book_search('CA')
    book_search('NL')
    book_search('DE')
    book_search('US')
    
####################################
# Call check_calibre_list Function #
####################################
run_check_calibre_list = False
if run_check_calibre_list:
    print ("Running run_check_calibre_list...")
    check_calibre_list()
    
##################################
# Call output_online_db Function #
##################################
run_output_online_db = False
if run_output_online_db:
    print ("Running output_online_db...")
    output_online_db()

##################################
# Call index_site_list Function #
##################################
run_index_site_list = False
if run_index_site_list:
    print ("Running index_site_list...")
    index_site_list('online.txt')

#####################################
# Call index_site_list_seq Function #
#####################################
run_index_site_list_seq = False
if run_index_site_list_seq:
    print ("Running index_site_list_seq...")
    index_site_list_seq('online.txt')

#############################
# Call build_index Function #
#############################
run_build_index = False
if run_build_index:
    print ("Running run_index_site_list...")
    build_index()

###########################
# Call get_stats Function #
###########################
run_get_stats = False
if run_get_stats:
    print ("Running get_stats...")
    get_stats()

###############################
# Call index_to_json Function #
###############################
#run_index_to_json = False
if run_index_to_json:
    print ("Running index_to_json...")
    index_to_json()


