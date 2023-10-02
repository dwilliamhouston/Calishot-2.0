import shodan
import sys
import sqlite3

############################
# Query Calibre by Country #
############################
def calibre_by_country(country):
    apiquery = 'calibre http.status:"200" country:"' + country + '"'

    try:
        results = api.search(apiquery, page = 1, limit=49)
 
        for result in results['matches']:
            serverip = '%s' % result['ip_str'] +  ':%s' % result['port']
            query = "insert into servers (IPAddress, Country) VALUES ('"+ serverip + "','" + country + "')"
            curr=conn.cursor()
            curr.execute(query)
        print ('Completed ', country)
    except shodan.APIError as e:
        print ('Error: %s' % e)
    return

############################################
# Setup Variables and Database Connections #
############################################
SHODAN_API_KEY = "sEsxRpsOrBGJANgG1q6qL46xv153NrSV"
api = shodan.Shodan(SHODAN_API_KEY)
conn = sqlite3.connect('shodan.db')

calibre_by_country('AU')
calibre_by_country('US')
calibre_by_country('GB')
calibre_by_country('NZ')
calibre_by_country('IE')
calibre_by_country('CA')
calibre_by_country('NL')
calibre_by_country('DE')

##############################
# Close Database Connections #
##############################
conn.commit()
conn.close
