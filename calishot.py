########################
# Main Application     #
# Python3 calishot.py  #
########################

from functions import import_urls_from_file, check_calibre_list, index_site_list, index_site_list, get_stats, index_site_list_seq, import_urls_from_file, check_calibre_list, check_calibre_site, build_index, index_to_json, diff, calibre_by_country, book_search, output_online_db
import logging
import threading
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
#run_search_by_country = False
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
    
##############################################
# Call book_search Function for each Country #
##############################################
#run_book_search = False
if run_book_search:
    print ("Running run_book_search...")
    logging.info("Running run_book_search...")
    x=threading.Thread(target=book_search,args=('AU'))
    x.start()
    x=threading.Thread(target=book_search, args=('NZ'))        
    x.start()
    x=threading.Thread(target=book_search, args=('IE'))        
    x.start()
    x=threading.Thread(target=book_search, args=('GB'))        
    x.start()
    x=threading.Thread(target=book_search, args=('CA'))        
    x.start()
    x=threading.Thread(target=book_search, args=('NL'))        
    x.start()
    x=threading.Thread(target=book_search, args=('DE'))        
    x.start()
    x=threading.Thread(target=book_search, args=('US'))        
    x.start()
    x=threading.Thread(target=book_search,args=('FR'))
    x.start()
    x=threading.Thread(target=book_search, args=('ES'))        
    x.start()
    x=threading.Thread(target=book_search, args=('IT'))        
    x.start()
    x=threading.Thread(target=book_search, args=('CH'))        
    x.start()
    x=threading.Thread(target=book_search, args=('RU'))        
    x.start()
    x=threading.Thread(target=book_search, args=('KR'))        
    x.start()
    x=threading.Thread(target=book_search, args=('JP'))        
    x.start()
    x=threading.Thread(target=book_search, args=('SG'))        
    x.start()
    x=threading.Thread(target=book_search, args=('HK'))        
    x.start()
    x=threading.Thread(target=book_search, args=('KE'))        
    x.start()
    x=threading.Thread(target=book_search, args=('SE'))        
    x.start()

####################################
# Call check_calibre_list Function #
####################################
#run_check_calibre_list = False
if run_check_calibre_list:
    print ("Running run_check_calibre_list...")
    logging.info("Running run_check_calibre_list...")
    check_calibre_list()
    
##################################
# Call output_online_db Function #
##################################
#run_output_online_db = False
if run_output_online_db:
    print ("Running output_online_db...")
    logging.info("Running output_online_db...")
    output_online_db()

##################################
# Call index_site_list Function #
##################################
#run_index_site_list = False
if run_index_site_list:
    print ("Running index_site_list...")
    logging.info("Running index_site_list...")
    index_site_list('online.txt')

#####################################
# Call index_site_list_seq Function #
#####################################
#run_index_site_list_seq = False
if run_index_site_list_seq:
    print ("Running index_site_list_seq...")
    logging.info("Running index_site_list_seq...")
    index_site_list_seq('online.txt')

#################################
# Call build_index_eng Function #
#################################
#run_build_index_eng = False
if run_build_index_eng:
    print ("Running run_build_index_eng...")
    logging.info("Running run_build_index_eng...")
    build_index()

###########################
# Call get_stats Function #
###########################
#run_get_stats = False
if run_get_stats:
    print ("Running get_stats...")
    logging.info("Running get_stats...")
    get_stats()

###############################
# Call index_to_json Function #
###############################
#run_index_to_json = False
if run_index_to_json:
    print ("Running index_to_json...")
    logging.info("Running index_to_json...")
    index_to_json()