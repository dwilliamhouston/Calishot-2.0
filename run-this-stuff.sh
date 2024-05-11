#!/bin/sh
docker build .
docker run -p 5000:5000 -v $(pwd)/app/data:/app/data $(docker images | sed -n 2p | sed 's/.*<none> *//' | sed 's/ .*//')
