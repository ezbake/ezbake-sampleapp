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


from __future__ import print_function

import argparse
import json
import sys

def main():
    if len(sys.argv) < 3:
        print('Usage: %s <json file> <json file> [json files...]',
              file=sys.stderr)

        return 1

    all_tweets = []
    for json_path in sys.argv[1:]:
        with open(json_path) as json_file:
            try:
                tweets = json.load(json_file)
                all_tweets.extend(tweets)
            except ValueError as err:
                print('File at %s is not valid JSON' % json_path,
                      file=sys.stderr)

                return 1

    print(json.dumps(all_tweets, indent=2))
    return 0

if __name__ == '__main__':
    sys.exit(main())
