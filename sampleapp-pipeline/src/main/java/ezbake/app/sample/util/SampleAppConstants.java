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

package ezbake.app.sample.util;

/**
 * Holds constants and helper methods for Sample App.
 */
public abstract class SampleAppConstants {
    public static final String TWEET_URI = "social://sampleapp/ingest/tweet/";
    public static final String IMAGE_URI = "social://sampleapp/ingest/image/";

    /**
     * Returns a URI for a tweet ID.
     *
     * @param tweetId ID of the tweet
     * @return URI for the passed tweetId
     */
    public static String getTweetUri(String tweetId) {
        return TWEET_URI + tweetId;
    }

    /**
     * Returns a URI for an image ID.
     *
     * @param imageId ID of the image
     * @return URI for the passed imageId
     */
    public static String getImageUri(String imageId) {
        return IMAGE_URI + imageId;
    }
}
