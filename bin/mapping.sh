#!/bin/sh

if [ -z "$ES_URL" ]; then
  ES_URL="http://weather-lab-elasticsearch:9200"
fi

echo "$(date) Mapping Using ES_URL $ES_URL"

until curl -XGET $ES_URL -H 'Content-Type: application/json'; do
  >&2 echo "$(date) Elasticsearch is unavailable - sleeping"
  sleep 5
done

>&2 echo "$(date) Elasticsearch is up - executing command"


# Index mapping
curl -XPUT $ES_URL/_template/weather-lab-mapping-template -H 'Content-Type: application/json' -d '{
    "index_patterns": ["weather_data*"],
    "mappings" : {
        "properties" : {
          "location" : {
            "type" : "geo_point"
          }
        }
      }
  }'
echo " "

echo "$(date) Mapping finished"