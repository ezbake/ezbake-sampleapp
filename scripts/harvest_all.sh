#!/bin/bash
#   Copyright (C) 2013-2014 Computer Sciences Corporation
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.

#Args : 1:File, 2:numtweets 3:consumer key 4: consumer secret 5: access token 6: access secret

USERNAME_FILE=$1
TWEETS_PER_USER=$2
CONSUMER_KEY=$3
CONSUMER_SECRET=$4
ACCESS_TOKEN=$5
ACCESS_SECRET=$6

echo "[]" > mergedTweets.json

while read NAME
do
   ./twitter_harvest.py --consumer-key "$CONSUMER_KEY" \
                        --consumer-secret "$CONSUMER_SECRET" \
                        --access-token "$ACCESS_TOKEN" \
                        --access-secret "$ACCESS_SECRET" \
                        --numtweets "$TWEETS_PER_USER" \
                        --user "$NAME" \
                        --json-output "$NAME".json \
                        --image-output-dir tweet_images

   ./combine_tweets.py "$NAME".json mergedTweets.json > temp

   #Cleanup generated files
   mv temp mergedTweets.json
   rm "$NAME".json
done < $USERNAME_FILE
