#!/bin/bash

# we don't want to drop insights api graphQL cache too frequently

CACHE_FLAG=/tmp/insights-cache-cleaned

# Check if the cache flag is older than 1 hour
if [ -z "$(find $CACHE_FLAG -mmin -60 2>/dev/null)" ]; then
    curl -s -o /dev/null -w '[%{response_code}] cleaned insights cache\n' $INSIGHTS_API_URL/cache/cleanUp

    touch $CACHE_FLAG
fi
