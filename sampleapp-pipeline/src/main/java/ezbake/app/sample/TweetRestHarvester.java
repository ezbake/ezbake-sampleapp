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

package ezbake.app.sample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.common.properties.EzProperties;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterObjectFactory;
import twitter4j.conf.ConfigurationBuilder;

/**
 * Simple class to harvest Tweets using the Twitter API.
 */
public abstract class TweetRestHarvester {
    private static final String OAUTH_ERR_MSG_BEGIN = "The following OAuth properties have not been assigned: ";

    /**
     * Property whose value should be a path to a file containing a list of Twitter user names.
     */
    private static final String TWITTER_USER_FILE_NAME = "twitter.user.file";

    /**
     * Property that indicates the number of Tweets to harvest from each user from the file indicated by the the
     * TWITTER_USER_FILE property.
     */
    private static final String TWEETS_PER_USER = "twitter.user.numtweets";
    private static final int DEFAULT_TWEETS_PER_USER = 10;

    /**
     * OAuth properties necessary for connecting to Twitter. Twitter supplies the values for these properties.
     */
    private static final String TWITTER_CONSUMER_KEY = "twitter.consumer.key";
    private static final String TWITTER_CONSUMER_SECRET = "twitter.consumer.secret";
    private static final String TWITTER_ACCESS_TOKEN = "twitter.access.token";
    private static final String TWITTER_ACCESS_SECRET = "twitter.access.secret";

    private static final Logger logger = LoggerFactory.getLogger(TweetRestHarvester.class);

    /**
     * Reads a list of Twitter users from a file and harvest tweets from them. Also depends on several OAuth properties
     * for connecting to Twitter.
     *
     * @param props - properties from EzConfig
     * @return JSONArray contains harvested tweets
     * @throws IOException if there is a problem retrieving and parsing the Twitter user file
     * @throws TwitterException if there is a problem while connecting/connected to Twitter
     * @throws JSONException if JSON received from Twitter is malformed/ cannot be parsed
     */
    public static JSONArray harvestTweets(Properties props) throws TwitterException, JSONException, IOException {
        final JSONArray rawJson = new JSONArray();
        checkProps(props);

        final String usernameFile = props.getProperty(TWITTER_USER_FILE_NAME);
        try (BufferedReader br = new BufferedReader(new FileReader(usernameFile))) {
            logger.info("Harvesting tweets from users found in: {}", usernameFile);
            String line = br.readLine();
            while (line != null) {
                addTweetsToJson(rawJson, line, props);
                line = br.readLine();
            }
        }

        return rawJson;
    }

    /**
     * Checks to make sure all necessary properties have been set, and provides detailed feedback about which properties
     * are missing.
     *
     * @param props EzBake configuration properties or necessary Twitter properties.
     */
    private static void checkProps(Properties props) {
        StringBuilder missingProps = new StringBuilder("");

        if (StringUtils.isEmpty(props.getProperty(TWITTER_USER_FILE_NAME))) {
            missingProps = missingProps.append(
                    "twitter.user.file property unassigned. Please assign the location of "
                            + "a file containing a list of twitter usernames to this property. ");
        }

        final String[] oauthProps =
                {TWITTER_CONSUMER_KEY, TWITTER_CONSUMER_SECRET, TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_SECRET};

        final StringBuilder oauthErrorMsg = new StringBuilder(OAUTH_ERR_MSG_BEGIN);
        for (final String prop : oauthProps) {
            if (StringUtils.isEmpty(props.getProperty(prop))) {
                oauthErrorMsg.append(prop);
                oauthErrorMsg.append(' ');
            }
        }

        final String currentMsg = oauthErrorMsg.toString();
        if (!currentMsg.equals(OAUTH_ERR_MSG_BEGIN)) {
            final StringBuilder missingPropsErrorMsg = new StringBuilder(currentMsg.trim());

            missingPropsErrorMsg.append(
                    ". For more information on what to assign these properties, "
                            + "please consult the project readme. ");

            missingProps.append(missingPropsErrorMsg);
        }

        if (!StringUtils.isEmpty(missingProps)) {
            throw new RuntimeException(missingProps.toString());
        }
    }

    /**
     * Connects to Twitter and harvests tweets for a particular Twitter user.
     *
     * @param rawJson A JSONArray which can be populated with Tweet JSONObjects
     * @param username A Twitter username from which to harvest tweets
     * @param props properties containing OAuth information for Twitter and possibly information about the number of
     * tweets to harvest
     * @throws TwitterException if there is a problem while connecting/connected to Twitter
     * @throws JSONException if JSON received from Twitter is malformed/ cannot be parsed.
     */
    private static void addTweetsToJson(JSONArray rawJson, String username, Properties props)
            throws TwitterException, JSONException {
        final EzProperties ezProps = new EzProperties(props, false);

        final ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true).setJSONStoreEnabled(true).setOAuthConsumerKey(props.getProperty(TWITTER_CONSUMER_KEY))
                .setOAuthConsumerSecret(props.getProperty(TWITTER_CONSUMER_SECRET))
                .setOAuthAccessToken(props.getProperty(TWITTER_ACCESS_TOKEN))
                .setOAuthAccessTokenSecret(props.getProperty(TWITTER_ACCESS_SECRET));

        final TwitterFactory tf = new TwitterFactory(cb.build());
        final Twitter twitter = tf.getInstance();
        final Query query = new Query("from:@" + username);
        logger.debug("Attempting to harvest tweets from: {}", username);

        query.setCount(ezProps.getInteger(TWEETS_PER_USER, DEFAULT_TWEETS_PER_USER));
        final QueryResult result = twitter.search(query);

        for (final Status status : result.getTweets()) {
            rawJson.put(new JSONObject(TwitterObjectFactory.getRawJSON(status)));
        }
    }
}
