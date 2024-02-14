########################
# Main Application     #
# Python3 calishot.py  #
########################

from functions import import_urls_from_file, check_calibre_list, index_site_list, index_site_list, get_stats, index_site_list_seq, import_urls_from_file, check_calibre_list, check_calibre_site, build_index, index_to_json, diff, calibre_by_country, book_search, output_online_db
import logging
logging.basicConfig(filename='shodantest.log', encoding='utf-8', level=logging.ERROR)

#####################
# Flags for testing #
#####################
run_search_by_country = True
run_book_search = True
run_check_calibre_list = True
run_output_online_db = True
run_index_site_list = True
run_index_site_list_seq = True
run_build_index_eng = True
run_get_stats = True
run_index_to_json = True

####################################################
# Call search_by_country Function for each Country #
####################################################
if run_search_by_country:
    print ("Running search_by_country...")
    logging.info("Running search_by_country...")
    calibre_by_country('AU')
    calibre_by_country('NZ')
    calibre_by_country('IE')
    calibre_by_country('GB')
    calibre_by_country('CA')
    calibre_by_country('NL')
    calibre_by_country('DE')
    calibre_by_country('US')
    calibre_by_country('FR')
    calibre_by_country('ES')
    calibre_by_country('IT')
    calibre_by_country('CH')
    calibre_by_country('RU')
    calibre_by_country('KR')
    calibre_by_country('JP')
    calibre_by_country('SG')
    calibre_by_country('HK')
    calibre_by_country('KE')
    calibre_by_country('SE')
    calibre_by_country('PL')
    calibre_by_country('RS')
    calibre_by_country('HU')
    calibre_by_country('TW')
    calibre_by_country('IN')
    calibre_by_country('FI')
    calibre_by_country('KR')
    calibre_by_country('BE')
    calibre_by_country('VN')
    calibre_by_country('ZA')
    calibre_by_country('AT')
    calibre_by_country('PT')
    calibre_by_country('UY')
    calibre_by_country('RO')
    calibre_by_country('SG')
    calibre_by_country('BR')
    calibre_by_country('CL')
    calibre_by_country('DK')
    calibre_by_country('HR')
    calibre_by_country('MX')
    calibre_by_country('AR')
    calibre_by_country('MY')
    calibre_by_country('IL')
    calibre_by_country('BG')
    calibre_by_country('CZ')
    calibre_by_country('LU')
    calibre_by_country('UA')
    calibre_by_country('MK')
    calibre_by_country('GR')
    calibre_by_country('MA')
    calibre_by_country('NG')
    calibre_by_country('BJ')
    calibre_by_country('EE')
    calibre_by_country('GH')
    calibre_by_country('ID')
    calibre_by_country('IS')
    calibre_by_country('JM')
    calibre_by_country('MO')
    calibre_by_country('PH')
    calibre_by_country('PK')
    calibre_by_country('RW')
    calibre_by_country('SI')
    calibre_by_country('SK')
    calibre_by_country('UG')
    calibre_by_country('UZ')
    
##############################################
# Call book_search Function for each Country #
##############################################
if run_book_search:
    print ("Running run_book_search...")
    logging.info("Running run_book_search...")
    book_search('AU')
    book_search('NZ')
    book_search('IE')
    book_search('GB')
    book_search('CA')
    book_search('NL')
    book_search('DE')
    book_search('US')
    book_search('FR')
    book_search('ES')
    book_search('IT')
    book_search('CH')
    book_search('RU')
    book_search('KR')
    book_search('JP')
    book_search('SG')
    book_search('HK')
    book_search('KE')
    book_search('SE')
    book_search('PL')
    book_search('RS')
    book_search('HU')
    book_search('TW')
    book_Search('IN')
    book_Search('FI')
    book_search('KR')
    book_Search('BE')
    book_Search('VN')
    book_search('ZA')
    book_Search('AT')
    book_Search('PT')
    book_Search('UY')
    book_Search('RO')
    book_Search('SG')
    book_Search('BR')
    book_Search('CL')
    book_Search('DK')
    book_Search('HR')
    book_Search('MX')
    book_Search('AR')
    book_Search('MY')
    book_Search('IL')
    book_Search('BG')
    book_Search('CZ')
    book_Search('LU')
    book_Search('UA')
    book_Search('MK')
    book_Search('GR')
    book_Search('MA')
    book_Search('NG')
    book_Search('BJ')
    book_Search('EE')
    book_Search('GH')
    book_Search('ID')
    book_Search('IS')
    book_Search('JM')
    book_Search('MO')
    book_Search('PH')
    book_Search('PK')
    book_Search('RW')
    book_Search('SI')
    book_Search('SK')
    book_Search('UG')
    book_Search('UZ')
    
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
    index_site_list('/app/data/online.txt')

#####################################
# Call index_site_list_seq Function #
#####################################
if run_index_site_list_seq:
    print ("Running index_site_list_seq...")
    logging.info("Running index_site_list_seq...")
    index_site_list_seq('/app/data/online.txt')

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
