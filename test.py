####################
# Main Application #
# Python3 test.py  #
####################

from functions import import_urls_from_file, check_calibre_list, index_site_list, index_site_list, get_stats, index_site_list_seq, import_urls_from_file, check_calibre_list, check_calibre_site, build_index_eng, build_index_noteng, index_to_json, diff, calibre_by_country, book_search, output_online_db

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
run_build_index_noteng = True
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

#################################
# Call build_index_eng Function #
#################################
#run_build_index_eng = False
if run_build_index_eng:
    print ("Running run_build_index_eng...")
    build_index_eng()

####################################
# Call build_index_noteng Function #
####################################
#run_build_index_noteng = False
#if run_build_index_noteng:
#    print ("Running run_build_index_noteng...")
#    build_index_noteng()

###########################
# Call get_stats Function #
###########################
#run_get_stats = False
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