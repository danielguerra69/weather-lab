#!/bin/sh

# Run the mapping script
/app/bin/mapping.sh

# Execute the main application
exec "$@"