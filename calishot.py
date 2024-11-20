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
run_index_site_list_seq = False
run_build_index_eng = True
run_get_stats = True
run_index_to_json = True

####################################################
# Call search_by_country Function for each Country #
####################################################
if run_search_by_country:
    print ("Running search_by_country...")
    logging.info("Running search_by_country...")
    #calibre_by_country('AU')
    #calibre_by_country('NZ')
    #calibre_by_country('IE')
    #calibre_by_country('GB')
    #calibre_by_country('CA')
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
    calibre_by_country('NO')
    calibre_by_country('KG')
    calibre_by_country('ET')
    calibre_by_country('BY')
    calibre_by_country('BO')
    calibre_by_country('BD')
    calibre_by_country('BA')
    calibre_by_country('AL')
    
    
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
    #book_search('CA')
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
    book_search('IN')
    book_search('FI')
    book_search('KR')
    book_search('BE')
    book_search('VN')
    book_search('ZA')
    book_search('AT')
    book_search('PT')
    book_search('UY')
    book_search('RO')
    book_search('SG')
    book_search('BR')
    book_search('CL')
    book_search('DK')
    book_search('HR')
    book_search('MX')
    book_search('AR')
    book_search('MY')
    book_search('IL')
    book_search('BG')
    book_search('CZ')
    book_search('LU')
    book_search('UA')
    book_search('MK')
    book_search('GR')
    book_search('MA')
    book_search('NG')
    book_search('BJ')
    book_search('EE')
    book_search('GH')
    book_search('ID')
    book_search('IS')
    book_search('JM')
    book_search('MO')
    book_search('PH')
    book_search('PK')
    book_search('RW')
    book_search('SI')
    book_search('SK')
    book_search('UG')
    book_search('UZ')
    book_search('NO')
    book_search('KG')
    book_search('ET')
    book_search('BY')
    book_search('BO')
    book_search('BD')
    book_search('BA')
    book_search('AL')
    
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
