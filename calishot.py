########################
# Main Application     #
# Python3 calishot.py  #
########################

from pathlib import Path
from functions import import_urls_from_file, check_calibre_list, index_site_list, get_stats, index_site_list_seq, import_urls_from_file, check_calibre_site, build_index, index_to_json, diff, calibre_by_country, book_search, output_online_db

import logging
import calishot_logging
# Initialize shared rotating file logger pinned to project CWD
from pathlib import Path as _Path
# Pin to the repository root (directory of this file), matching calishot_web/app.py
calishot_logging.init_logging(logging.INFO, log_file=_Path(__file__).resolve().parent / 'calishot.log')

#####################
# Flags for testing #
#####################
run_search_by_country = True
run_book_search = True
run_check_calibre_list = True
run_output_online_db = True
run_index_site_list = True
run_index_site_list_seq = False
run_build_index_eng = Truee
run_get_stats = True
run_index_to_json = True

####################################################
# Call search_by_country Function for each Country #
####################################################
if run_search_by_country:
    print("Running search_by_country...")
    logging.info("Running search_by_country...")
    
    # Complete list of ISO 3166-1 alpha-2 country codes
    country_codes = [
   'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AR', 'AT', 'AU', 'AZ',
        'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BN', 'BO', 'BR',
        'BS', 'BT', 'BW', 'BY', 'BZ', 'CA', 'CD', 'CF', 'CG', 'CH', 'CI', 'CL',
        'CM', 'CN', 'CO', 'CR', 'CU', 'CV', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM',
        'DO', 'DZ', 'EC', 'EE', 'EG', 'ER', 'ES', 'ET', 'FI', 'FJ', 'FM', 'FR',
        'GA', 'GB', 'GD', 'GE', 'GH', 'GM', 'GN', 'GQ', 'GR', 'GT', 'GW', 'GY',
        'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IN', 'IQ', 'IR', 'IS', 'IT',
        'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KM', 'KN', 'KP', 'KR', 'KW',
        'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS', 'LT', 'LU', 'LV', 'LY',
        'MA', 'MC', 'MD', 'ME', 'MG', 'MH', 'MK', 'ML', 'MM', 'MN', 'MR', 'MT',
        'MU', 'MV', 'MW', 'MX', 'MY', 'MZ', 'NA', 'NE', 'NG', 'NI', 'NL', 'NO',
        'NP', 'NR', 'NZ', 'OM', 'PA', 'PE', 'PG', 'PH', 'PK', 'PL', 'PT', 'PW',
        'PY', 'QA', 'RO', 'RS', 'RU', 'RW', 'SA', 'SB', 'SC', 'SD', 'SE', 'SG',
        'SI', 'SK', 'SL', 'SM', 'SN', 'SO', 'SR', 'SS', 'ST', 'SV', 'SY', 'SZ',
        'TD', 'TG', 'TH', 'TJ', 'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TV', 'TW',
        'TZ', 'UA', 'UG', 'US', 'UY', 'UZ', 'VA', 'VC', 'VE', 'VN', 'VU', 'WS',
        'YE', 'ZA', 'ZM', 'ZW', 'AX', 'AS', 'AQ', 'AW', 'BM', 'BQ', 'BV', 'IO',
        'KY', 'CX', 'CC', 'CK', 'FO', 'GS', 'HM', 'SJ', 'SS', 'TF', 'UM', 'FK',
        'FO', 'GF', 'PF', 'GI', 'GL', 'GP', 'GU', 'GG', 'HK', 'IM', 'JE', 'MO',
        'MQ', 'YT', 'MS', 'NC', 'NU', 'NF', 'MP', 'PS', 'PN', 'PR', 'RE', 'BL',
        'SH', 'MF', 'PM', 'SX', 'GS', 'SJ', 'TK', 'TC', 'VG', 'VI', 'WF', 'EH',
        'AX'
    ]
    
    print(f"Processing {len(country_codes)} countries...")
    logging.info(f"Processing {len(country_codes)} countries")
    
    # Process each country code
    for country_code in sorted(country_codes):
        print(f"\nProcessing country: {country_code}")
        logging.info(f"Processing country: {country_code}")
        calibre_by_country(country_code)
    
##############################################
# Call book_search Function for each Country #
##############################################
if run_book_search:
    print("Running run_book_search...")
    logging.info("Running run_book_search...")
    
    # Complete list of ISO 3166-1 alpha-2 country codes
    country_codes = [
        'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AR', 'AT', 'AU', 'AZ',
        'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BN', 'BO', 'BR',
        'BS', 'BT', 'BW', 'BY', 'BZ', 'CA', 'CD', 'CF', 'CG', 'CH', 'CI', 'CL',
        'CM', 'CN', 'CO', 'CR', 'CU', 'CV', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM',
        'DO', 'DZ', 'EC', 'EE', 'EG', 'ER', 'ES', 'ET', 'FI', 'FJ', 'FM', 'FR',
        'GA', 'GB', 'GD', 'GE', 'GH', 'GM', 'GN', 'GQ', 'GR', 'GT', 'GW', 'GY',
        'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IN', 'IQ', 'IR', 'IS', 'IT',
        'JM', 'JO', 'JP', 'KE', 'KG', 'KH', 'KI', 'KM', 'KN', 'KP', 'KR', 'KW',
        'KZ', 'LA', 'LB', 'LC', 'LI', 'LK', 'LR', 'LS', 'LT', 'LU', 'LV', 'LY',
        'MA', 'MC', 'MD', 'ME', 'MG', 'MH', 'MK', 'ML', 'MM', 'MN', 'MR', 'MT',
        'MU', 'MV', 'MW', 'MX', 'MY', 'MZ', 'NA', 'NE', 'NG', 'NI', 'NL', 'NO',
        'NP', 'NR', 'NZ', 'OM', 'PA', 'PE', 'PG', 'PH', 'PK', 'PL', 'PT', 'PW',
        'PY', 'QA', 'RO', 'RS', 'RU', 'RW', 'SA', 'SB', 'SC', 'SD', 'SE', 'SG',
        'SI', 'SK', 'SL', 'SM', 'SN', 'SO', 'SR', 'SS', 'ST', 'SV', 'SY', 'SZ',
        'TD', 'TG', 'TH', 'TJ', 'TL', 'TM', 'TN', 'TO', 'TR', 'TT', 'TV', 'TW',
        'TZ', 'UA', 'UG', 'US', 'UY', 'UZ', 'VA', 'VC', 'VE', 'VN', 'VU', 'WS',
        'YE', 'ZA', 'ZM', 'ZW', 'AX', 'AS', 'AQ', 'AW', 'BM', 'BQ', 'BV', 'IO',
        'KY', 'CX', 'CC', 'CK', 'FO', 'GS', 'HM', 'SJ', 'SS', 'TF', 'UM', 'FK',
        'FO', 'GF', 'PF', 'GI', 'GL', 'GP', 'GU', 'GG', 'HK', 'IM', 'JE', 'MO',
        'MQ', 'YT', 'MS', 'NC', 'NU', 'NF', 'MP', 'PS', 'PN', 'PR', 'RE', 'BL',
        'SH', 'MF', 'PM', 'SX', 'GS', 'SJ', 'TK', 'TC', 'VG', 'VI', 'WF', 'EH',
        'AX'
    ]
    
    print(f"Processing {len(country_codes)} countries...")
    logging.info(f"Processing {len(country_codes)} countries")
    
    # Process each country code
    for country_code in sorted(country_codes):
        print(f"\nProcessing country: {country_code}")
        logging.info(f"Processing country: {country_code}")
        book_search(country_code)
    
    # Process other.txt if it exists in the data directory
    other_file = Path("data/other.txt")
    if other_file.exists():
        print("\nProcessing data/other.txt...")
        logging.info("Processing data/other.txt")
        import_urls_from_file("data/other.txt", country="other")
    else:
        print("\ndata/other.txt not found, skipping...")
        logging.info("data/other.txt not found, skipping...")

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