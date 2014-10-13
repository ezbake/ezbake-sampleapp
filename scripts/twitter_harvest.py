#!/usr/bin/env python
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


##############################################################################
#
# Copyright (c) 2013 ObjectLabs Corporation
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
# NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
# LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
# WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
################################################################################

# Modified to download images and to save to filesystem instead of MongoDB

from __future__ import print_function

import argparse
import json
import os
import sys
import time
import urllib
import urllib2

import oauth2

def oauth_header(url, consumer, token):
    params = {
        'oauth_version': '1.0',
        'oauth_nonce': oauth2.generate_nonce(),
        'oauth_timestamp': int(time.time())}

    req = oauth2.Request(method='GET', url=url, parameters=params)
    req.sign_request(oauth2.SignatureMethod_HMAC_SHA1(), consumer, token)
    return req.to_header()['Authorization'].encode('utf-8')

def main():
    ### Build arg parser
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=('Connects to Twitter User Timeline endpoint, retrieves '
                     'tweets and inserts into a MongoDB database. '
                     'Developed on Python 2.7'))

    parser.add_argument(
        '-r', '--retweet',
        help='Include native retweets in the harvest', action='store_true')

    parser.add_argument(
        '-v', '--verbose',
        help='Print harvested tweets in shell', action='store_true')

    parser.add_argument(
        '--json-output',
        help=(
            'Output path to store the JSON file of harvested tweets. '
            'Default: <user>.json'))

    parser.add_argument(
        '--image-output-dir',
        help='Output path to store the images found in harvested tweets',
        default=os.path.join(os.getcwd(), 'tweet_images'))

    parser.add_argument(
        '--numtweets',
        help='Set total number of tweets to be harvested, max = 3200',
        type=int, default=3200)

    parser.add_argument(
        '--user',
        help='Choose twitter user timeline for harvest', required=True)

    parser.add_argument(
        '--consumer-key',
        help='Consumer Key from your Twitter App OAuth settings', required=True)

    parser.add_argument(
        '--consumer-secret',
        help='Consumer Secret from your Twitter App OAuth settings',
        required=True)

    parser.add_argument(
        '--access-token',
        help='Access Token from your Twitter App OAuth settings', required=True)

    parser.add_argument(
        '--access-secret',
        help='Access Token Secret from your Twitter App Dev Credentials',
        required=True)

    ### Fields for query
    args = parser.parse_args()
    user = args.user
    numtweets = args.numtweets
    verbose = args.verbose
    retweet = args.retweet

    ### Build Signature
    consumer_key = args.consumer_key
    consumer_secret = args.consumer_secret
    access_token = args.access_token
    access_secret = args.access_secret

    ### Build Endpoint + Set Headers
    base_url = url = (
        'https://api.twitter.com/1.1/statuses/user_timeline.json?'
        'include_entities=true&count=200&screen_name=%s&include_rts=%s' %
        (user, retweet))

    oauth_consumer = oauth2.Consumer(key=consumer_key, secret=consumer_secret)
    oauth_token = oauth2.Token(key=access_token, secret=access_secret)

    ### Helper Variables for Harvest
    max_id = -1
    tweet_count = 0

    ### Begin Harvesting
    all_tweets = []
    continue_paging = True
    while continue_paging:
        auth = oauth_header(url, oauth_consumer, oauth_token)
        headers = {'Authorization': auth}
        request = urllib2.Request(url, headers=headers)
        try:
            stream = urllib2.urlopen(request)
        except urllib2.HTTPError as err:
            if err.code == 404:
                print('Error: Unknown user. Check --user arg', file=sys.stderr)
                return 1

            if err.code == 401:
                print('Error: Unauthorized. Check Twitter credentials',
                      file=sys.stderr)
	
                return 1
	    
            if err.code == 400:
                print('Error: Bad Request. Double check args.',
                      file=sys.stderr)
    
            return 1

            print(err, file=sys.stderr)
            return 1

        tweet_list = json.load(stream)

        if not tweet_list:
            print('No tweets to harvest!')
            break

        if max_id == -1:
            tweets = tweet_list
        else:
            tweets = tweet_list[1:]
            if not tweets:
                print('Finished Harvest!')
                break

        if 'errors' in tweet_list:
            print('Hit rate limit, code: %s, message: %s' %
                  (tweets['errors']['code'], tweets['errors']['message']),
                  file=sys.stderr)

            return 1

        all_tweets.extend(tweets)

        for tweet in tweets:
            max_id = tweet['id_str']
            try:
                if tweet_count == numtweets:
                    print('Finished Harvest- hit numtweets!')
                    continue_paging = False
                    break

                tweet_count += 1
            except Exception as err:
                print('Unexpected error encountered: %s' % err, file=sys.stderr)
                return 1

        url = base_url + '&max_id=' + max_id

    tweets_json = json.dumps(all_tweets, indent=2)

    if verbose:
        print(tweets_json)

    if args.json_output:
        json_output = args.json_output
    else:
        json_output = user + '.json'

    with open(json_output, 'w') as json_out:
        json_out.write(tweets_json)

    if not os.path.exists(args.image_output_dir):
        os.mkdir(args.image_output_dir)

    print('Downloading images linked from tweets...')
    for tweet in all_tweets:
        if 'entities' not in tweet:
            continue

        if 'media' not in tweet['entities']:
            continue

        for media_file in tweet['entities']['media']:
            if media_file['type'] != 'photo':
                continue

            image_url = media_file['media_url']
            image_id = media_file['id_str']
            extension = os.path.splitext(image_url)[1]
            image_file_name = image_id + extension
            image_path = os.path.join(args.image_output_dir, image_file_name)
            urllib.urlretrieve(image_url, image_path)
            print('Downloaded image %s to %s' % (image_url, image_path))

    return 0

if __name__ == '__main__':
    sys.exit(main())
