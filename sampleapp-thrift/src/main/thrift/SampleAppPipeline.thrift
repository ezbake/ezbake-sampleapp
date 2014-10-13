/*   Copyright (C) 2013-2014 Computer Sciences Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License. */

include "ImageMetadataExtractor.thrift"

namespace * ezbake.app.sample.thrift

typedef ImageMetadataExtractor.Image Image

/**
 * A Twitter user's basic information
 */
struct TwitterUser {
    /**
     * Unique ID
     */
    1: required string id;

    /**
     * Twitter screen name as used in mentions, etc. This is @screen_name without the "@" sign.
     */
    2: required string screenName;

    /**
     * Display name that may or may not be the user's real name
     */
    3: optional string name;
}

/**
 * A tweet referenced (e.g., by replying or retweeting) from another tweet
 */
struct ReferencedTweet {
    /**
     * ID of the referenced tweet
     */
    1: required string id;

    /**
     * Author of the referenced tweet
     */
    2: required TwitterUser author;
}

/**
 * Extracted and normalized information from the tweet JSON from the Twitter REST API
 */
struct Tweet {
    /**
     * Raw JSON from Twitter API. New keys may be added for application-specific purposes
     */
    1: required string rawJson;

    /**
     * Map of EzBake image IDs to Image (with binary, etc.)
     */
    2: required map<string, Image> images;

    /**
     * Tweet ID
     */
    3: required string id;

    /**
     * User info of the author of the tweet
     */
    4: required TwitterUser author;

    /**
     * Info for the users mentioned by this tweet in its text
     */
    5: required list<TwitterUser> mentionedUsers;

    /**
     * Info for the tweet that was retweeted, if applicable
     */
    6: optional ReferencedTweet retweeted;

    /**
     * Info for the tweet to which this tweet was a reply, if applicable
     */
    7: optional ReferencedTweet repliedTo;

    /**
     * Provenance ID for the tweet
     */
    8: required i64 provenanceId;
}
