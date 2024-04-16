Application to search Shodan for Calibre Web sites that and pull all books into a database and then display on a website.

This code is based on code created by Krazybug - https://github.com/Krazybug/calishot

<B>Instructions if you want to use existing or downloaded index.db locally (Not using Docker)</B>

  Step 1 - Setup new python environemnt and install datasette. 

  python -m venv shodantest
  ../calishot/bin/activate
  pip install datasette
  pip install datasette-json-html
  pip install datasette-pretty-json

  Step 2 - Execute from command line: pip -r install requirements.txt

  Step 3 - download index.db into your venv directory.

  Step 4 - Execute from command line:
  datasette serve index.db --config sql_time_limit_ms:50000 --config allow_download:off --config max_returned_rows:2000  --config num_sql_threads:10 --config allow_csv_stream:off  --metadata       metadata.json

<B>Instructions if using Docker rather than setting up your own Python environment:</B>

  Step 1 - Create a directory called /app and then a directory called /app/data and put the index.db file in it. 

  Step 2 - docker -v /app/data:/app/data run -p 5001:5000 dwilliamhouston/shodantest:latest datasette -p 5000 -h 0.0.0.0 /app/data/index.db --config sql_time_limit_ms:50000 --config allow_download:off --config max_returned_rows:2000  --config num_sql_threads:10 --config allow_csv_stream:off  --metadata metadata.json

  Step 3 - open the browser to http://localhost:5001
  
