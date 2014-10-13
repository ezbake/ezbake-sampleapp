# Usage

### On initial clone
git submodule update --init

### twitter_harvest.py
#### Mac OS X Setup
1. Make sure PIP is installed by either:
  * Installing Python via Homebrew: ```brew install python```
  * ```sudo easy_install pip```
2. ```sudo pip install oauth2```

#### EzCentOS Setup
1. ```sudo yum install python-pip```
2. ```sudo pip install oauth2 argparse```

#### Running the Script

The script can be run as follows to harvest a number of tweets from a specific Twitter
user.  In this case, 500 tweets from @BBCBreaking. 

        ./twitter_harvest.py --consumer-key <consumer key> \
                             --consumer-secret <consumer secret> \ 
                             --access-token <access token> \
                             --access-secret <access token secret> \
                             --numtweets 500 \
                             --user BBCBreaking
   
By default, harvested tweets will be placed in a file based on the twitter
user, specifically the tweets will be placed in '<username>.json' and images from 
those tweets will be placed in a directory 'tweet_images/'.

### Helper Scripts
#### combine_tweets.py

Run the 'combine\_tweets.py' script to combine json files output by the twitter_harvest 
script.  E.g. To harvest 500 tweets from @BBCBreaking and @CNN and then combine the result
into a single file 'tweets.json' enter the following commands:
       
        ./twitter_harvest.py --consumer-key <consumer key> \
                             --consumer-secret <consumer secret> \ 
                             --access-token <access token> \
                             --access-secret <access token secret> \
                             --numtweets 500 \
                             --user BBCBreaking

        ./twitter_harvest.py --consumer-key <consumer key> \
                             --consumer-secret <consumer secret> \ 
                             --access-token <access token> \
                             --access-secret <access token secret> \
                             --numtweets 500 \
                             --user CNN

        ./combine_tweets.py BBCBreaking.json CNN.json > tweets.json

NOTE: If the output file already exists, its contents will be overwritten.

#### harvest_all.py

This script uses a file that contains a list of Twitter users, harvests a specified
number of tweets from each user, and combines the results into a single output file 
'merged_tweets.json'.

1. Create a file (this one is named 'news_sources.txt') of an unlimited number of Twitter 
users with one user per each line. For example:

        BBCBreaking
        CNN
        washingtonpost

2. Enter the following command to download 500 tweets and the associated images from each of the
 users in the 'news_sources.txt' file.  These tweets will be combined into a single json
file called 'mergedTweets.json'.

        ./harvest_all.sh news_sources.txt 500 \
             <consumer key> <consumer secret> \
             <access token> <access secret>

NOTE: The file 'mergeTweets.json' will be overwritten if it already exists.