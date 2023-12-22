Application to search Shodan for Calibre Web sites that and pull all books into a database and then display on a website.

This code is based on code created by Krazybug - https://github.com/Krazybug/calishot

Instructions

Step 1 - Setup new python environemnt and install datasette. 

python -m venv shodantest
. ./calishot/bin/activate
pip install datasette
pip install datasette-json-html
pip install datasette-pretty-json

Step 2 - Get code from Github https://github.com/dwilliamhouston/ShodanTest

Step 3 - Update SHODAN_API_KEY = "Enter SHODAN API KEY here from your Shodan Account" to your own SHODAN API KEY in functions.py

Step 4 - Execute from command line: pip -r install requirements.txt

Step 5 - Execute from command line python3 test.py

Step 6 - Execute from command line:
datasette serve index-non-eng.db --config sql_time_limit_ms:50000 --config allow_download:off --config max_returned_rows:2000  --config num_sql_threads:10 --config allow_csv_stream:off  --metadata metadata.json

Step 7 - Open your browser to http://localhost:8001/ and check the result
