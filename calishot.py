########################
# Main Application     #
# Python3 calishot.py  #
########################

from functions import import_urls_from_file, check_calibre_list, index_site_list, get_stats, index_site_list_seq, import_urls_from_file, check_calibre_site, build_index, index_to_json, diff, calibre_by_country, book_search, output_online_db

import logging
logging.basicConfig(filename='shodantest.log', encoding='utf-8', level=logging.ERROR)

#####################
# Flags for testing #
#####################
run_search_by_country_test = False
run_search_by_country = True
run_book_search = True
run_check_calibre_list = True
run_output_online_db = True
run_index_site_list = True
run_index_site_list_seq = False
run_build_index_eng = True
run_get_stats = True
run_index_to_json = True

####################################################
# Call search_by_country Function for each Country #
####################################################
if run_search_by_country_test:
    print ("Running search_by_country...")
    logging.info("Running search_by_country...")
    calibre_by_country('US')  

if run_search_by_country:
    print ("Running search_by_country...")
    logging.info("Running search_by_country...")
    calibre_by_country('US')    
    calibre_by_country('CN')
    calibre_by_country('DE')
    calibre_by_country('GB')
    calibre_by_country('SG')
    calibre_by_country('JP')
    calibre_by_country('ID')
    calibre_by_country('CA')
    calibre_by_country('HK')
    calibre_by_country('IT')
    calibre_by_country('HK')
    calibre_by_country('AE')
    calibre_by_country('ES')
    calibre_by_country('IT')
    calibre_by_country('KR')
    calibre_by_country('OM')
    calibre_by_country('FR')
    calibre_by_country('NL')
    calibre_by_country('TH')
    calibre_by_country('PH')
    calibre_by_country('MY')
    calibre_by_country('AU')
    calibre_by_country('IQ')
    calibre_by_country('SA')
    calibre_by_country('PK')
    calibre_by_country('NP')
    calibre_by_country('ZA')
    calibre_by_country('IN')
    calibre_by_country('RU')
    calibre_by_country('NZ')
    calibre_by_country('QA')
    calibre_by_country('AR')
    calibre_by_country('MX')
    calibre_by_country('TW')
    calibre_by_country('CH')
    calibre_by_country('CL')
    calibre_by_country('HU')
    calibre_by_country('KE')
    calibre_by_country('SE')
    calibre_by_country('BR')
    calibre_by_country('VN')
    calibre_by_country('BE')
    calibre_by_country('IE')
    calibre_by_country('NO')
    calibre_by_country('PE')
    calibre_by_country('AT')
    calibre_by_country('BA')
    calibre_by_country('BG')
    calibre_by_country('BH')
    calibre_by_country('BM')
    calibre_by_country('CZ')
    calibre_by_country('EG')
    calibre_by_country('FI')
    calibre_by_country('GR')
    calibre_by_country('KH')
    calibre_by_country('ME')
    calibre_by_country('PL')
    calibre_by_country('PT')
    calibre_by_country('RS')
    calibre_by_country('RW')
    calibre_by_country('UA')
    
##############################################
# Call book_search Function for each Country #
##############################################
if run_book_search:
    print ("Running run_book_search...")
    logging.info("Running run_book_search...")
    book_search('US')
    book_search('CN')
    book_search('DE')
    book_search('GB')
    book_search('ES')
    book_search('GB')
    book_search('SG')
    book_search('JP')
    book_search('ID')
    book_search('CA')
    book_search('HK')
    book_search('AE')
    book_search('ES')
    book_search('IT')
    book_search('KR')
    book_search('OM')
    book_search('FR')
    book_search('NL')
    book_search('TH')
    book_search('PH')
    book_search('MY')
    book_search('AU')
    book_search('IQ')
    book_search('SA')
    book_search('PK')
    book_search('NP')
    book_search('ZA')
    book_search('IN')
    book_search('RU')
    book_search('NZ')
    book_search('QA')
    book_search('AR')
    book_search('MX')
    book_search('TW')
    book_search('CH')
    book_search('CL')
    book_search('HU')
    book_search('KE')
    book_search('SE')
    book_search('BR')
    book_search('VN')
    book_search('BE')
    book_search('IE')
    book_search('NO')
    book_search('PE')
    book_search('AT')
    book_search('BA')
    book_search('BG')
    book_search('BH')
    book_search('BM')
    book_search('CZ')
    book_search('EG')
    book_search('FI')
    book_search('GR')
    book_search('KH')
    book_search('ME')
    book_search('PL')
    book_search('PT')
    book_search('RS')
    book_search('RW')
    book_search('UA')
    
####################################
# Call check_calibre_list Function #
####################################
if run_check_calibre_list:
    print ("Running run_check_calibre_list...")
    logging.info("Running run_check_calibre_list...")
    check_calibre_list()
    
##################################
# Call output_online_db Function #
##################################
if run_output_online_db:
    print ("Running output_online_db...")
    logging.info("Running output_online_db...")
    output_online_db()

##################################
# Call index_site_list Function #
##################################
if run_index_site_list:
    print ("Running index_site_list...")
    logging.info("Running index_site_list...")
    index_site_list('./data/online.txt')

#####################################
# Call index_site_list_seq Function #
#####################################
if run_index_site_list_seq:
    print ("Running index_site_list_seq...")
    logging.info("Running index_site_list_seq...")
    index_site_list_seq('./data/online.txt')

#################################
# Call build_index_eng Function #
#################################
if run_build_index_eng:
    print ("Running run_build_index_eng...")
    logging.info("Running run_build_index_eng...")
    build_index()

###########################
# Call get_stats Function #
###########################
if run_get_stats:
    print ("Running get_stats...")
    logging.info("Running get_stats...")
    get_stats()

###############################
# Call index_to_json Function #
###############################
if run_index_to_json:
    print ("Running index_to_json...")
    logging.info("Running index_to_json...")
    index_to_json()
