#!/bin/bash

docker-compose pull
docker-compose build
read -p "Enter your CMEMS username: " CMEMS_USERNAME
read -sp "Enter your CMEMS password: " CMEMS_PASSWORD
echo ""

# Create .env file with credentials
cat > .env << EOL
CMEMS_USERNAME=${CMEMS_USERNAME}
CMEMS_PASSWORD=${CMEMS_PASSWORD}
EOL

echo "Credentials saved to .env file"
docker-compose up -d
echo "Weather Lab started"
echo "Logs can be viewed with 'docker-compose logs -f'"
