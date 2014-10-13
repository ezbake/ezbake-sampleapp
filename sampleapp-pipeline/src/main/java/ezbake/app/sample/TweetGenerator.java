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

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.app.sample.thrift.Tweet;
import ezbake.app.sample.util.SampleAppConstants;
import ezbake.app.sample.util.TweetParserUtils;
import ezbake.base.thrift.AdvancedMarkings;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.Visibility;
import ezbake.common.properties.EzProperties;
import ezbake.frack.api.Generator;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.extractor.imagemetadata.thrift.Image;
import ezbake.services.provenance.thrift.ProvenanceAgeOffRuleNotFoundException;
import ezbake.services.provenance.thrift.ProvenanceCircularInheritanceNotAllowedException;
import ezbake.services.provenance.thrift.ProvenanceDocumentExistsException;
import ezbake.services.provenance.thrift.ProvenanceParentDocumentNotFoundException;
import ezbake.services.provenance.thrift.ProvenanceService;
import ezbake.thrift.ThriftClientPool;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

/**
 * Base class for Tweet pipeline generators.
 */
public abstract class TweetGenerator extends Generator<Tweet> {
    private static final long serialVersionUID = 1302576509845872154L;

    private static final String PAUSE_PROP = "processing.pauseMilliseconds";
    private static final int DEFAULT_PAUSE_MILLISECONDS = 100;

    private static final Logger logger = LoggerFactory.getLogger(TweetGenerator.class);

    /**
     * Used to make provenance calls.
     */
    private EzSecurityToken token;

    /**
     * Pool from which to get the EzProvenance client.
     */
    private ThriftClientPool pool;
    private JSONArray tweetsJson;
    private int nextIndex;
    private int pauseMilliseconds;

    /**
     * Creates a {@link Visibility} for a Tweet given its source program name.
     *
     * @param tweetJson Tweet JSON
     * @return {@link Visibility} based on Tweet JSON
     * @throws JSONException if the source program name could not be retrieved
     */
    private static Visibility createVisibility(JSONObject tweetJson) throws JSONException {
        String formalVisibility = null;
        final String sourceProgramName = TweetParserUtils.getSourceProgramName(tweetJson);
        if (sourceProgramName.startsWith("Twitter")) {
            formalVisibility = "U";
        } else if ("web".equals(sourceProgramName)) {
            formalVisibility = "U&FOUO";
        } else if (sourceProgramName.startsWith("Tweetbot")) {
            formalVisibility = "C";
        } else if ("Instagram".equals(sourceProgramName)) {
            formalVisibility = "S";
        } else if ("TweetDeck".equals(sourceProgramName)) {
            formalVisibility = "TS";
        } else if ("Facebook".equals(sourceProgramName)) {
            formalVisibility = "TS&USA";
        } else {
            formalVisibility = "TS&(USA|GBR)";
        }

        final AdvancedMarkings markings = new AdvancedMarkings();
        markings.setComposite(TweetParserUtils.hasImages(tweetJson));

        final Visibility visibility = new Visibility();
        visibility.setFormalVisibility(formalVisibility);
        visibility.setAdvancedMarkings(markings);

        return visibility;
    }

    @Override
    public final void initialize(Properties props) {
        try {
            pool = new ThriftClientPool(props);
            final String securityId = new EzBakeApplicationConfigurationHelper(props).getSecurityID();

            try (EzbakeSecurityClient securityClient = new EzbakeSecurityClient(props)) {
                token = securityClient.fetchAppToken(securityId);
            } catch (final IOException e) {
                final String errMsg = "Could not close security client";
                logger.error(errMsg, e);
                throw new RuntimeException(errMsg, e);
            }
        } catch (final EzSecurityTokenException e) {
            final String errMsg = "EzSecurity token error";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }

        pauseMilliseconds = new EzProperties(props, false).getInteger(PAUSE_PROP, DEFAULT_PAUSE_MILLISECONDS);
        tweetsJson = initTweets(props);
        logger.info("Pipeline initialized to process {} Tweets", tweetsJson.length());
    }

    @Override
    public final void generate() {
        if (nextIndex < tweetsJson.length()) {
            logger.info("generate() called. nextIndex={}, JSON array length={}", nextIndex, tweetsJson.length());
            try {
                final JSONObject tweetJson = tweetsJson.getJSONObject(nextIndex);
                final Tweet tweet = parseTweet(tweetJson);
                final Visibility visibility = createVisibility(tweetJson);
                logger.info("Setting Tweet to have formal visibility: {}", visibility.getFormalVisibility());
                outputToPipes(visibility, tweet);
            } catch (final JSONException e) {
                logger.error("Invalid Tweet JSON", e);
            } catch (final IOException e) {
                logger.error("File I/O error", e);
            } catch (final NoSuchAlgorithmException e) {
                logger.error("Could not create image hash", e);
            }

            nextIndex++;
        } else if (nextIndex == tweetsJson.length()) {
            logger.info("Finished processing Tweets");
            nextIndex++;
        }

        pause();
    }

    /**
     * Get Tweets from source and other generator-specific initialization.
     *
     * @param props Configuration properties
     * @return A JSON array of Tweet JSON objects
     */
    protected abstract JSONArray initTweets(Properties props);

    /**
     * Reads the images referenced by the Tweet JSON.
     *
     * @param tweetJson Tweet JSON
     * @return a {@link Map} of EzBake image IDs to images
     * @throws JSONException if parsing failed
     * @throws IOException if an image referenced in the Tweet could not be read
     * @throws NoSuchAlgorithmException if an image referenced in the Tweet could not be hashed
     */
    protected abstract Map<String, Image> parseImages(JSONObject tweetJson)
            throws JSONException, IOException, NoSuchAlgorithmException;

    /**
     * Parses the JSON of a Tweet and creates a {@link Tweet} object.
     *
     * @param tweetJson Tweet JSON
     * @return {@link Tweet} object
     * @throws JSONException if parsing failed
     * @throws IOException if an image referenced in the Tweet could not be read
     * @throws NoSuchAlgorithmException if an image referenced in the Tweet could not be hashed
     */
    private Tweet parseTweet(JSONObject tweetJson) throws JSONException, IOException, NoSuchAlgorithmException {
        final Tweet tweet = new Tweet();
        final long provenanceId = getProvenanceId(tweetJson);
        tweet.setImages(parseImages(tweetJson));
        tweet.setId(TweetParserUtils.getId(tweetJson));
        tweet.setAuthor(TweetParserUtils.getAuthor(tweetJson));
        tweet.setMentionedUsers(TweetParserUtils.getMentionedUsers(tweetJson));
        tweet.setRetweeted(TweetParserUtils.getRetweeted(tweetJson));
        tweet.setRepliedTo(TweetParserUtils.getRepliedTo(tweetJson));
        tweet.setProvenanceId(provenanceId);

        // Update JSON with EzBake-generated provenance ID
        TweetParserUtils.setEzBakeProvenanceId(tweetJson, provenanceId);

        // Update JSON with EzBake-generated image IDs
        TweetParserUtils.setEzBakeImageIds(tweetJson, tweet.getImages().keySet());
        tweet.setRawJson(tweetJson.toString());

        return tweet;
    }

    /**
     * Calls provenance service with Tweet information to get an ID for the Tweet that is unique across EzBake.
     *
     * @param tweetJson Tweet JSON
     * @return provenance ID from EzBake Provenance Service (long)
     * @throws JSONException if parsing failed
     */
    private long getProvenanceId(JSONObject tweetJson) throws JSONException {
        final long provenanceId;
        ProvenanceService.Client provenanceServiceClient = null;
        try {
            provenanceServiceClient = pool.getClient("EzProvenanceService", ProvenanceService.Client.class);
            final String provenanceURI = SampleAppConstants.getTweetUri(TweetParserUtils.getId(tweetJson));
            logger.info("Registering {} with provenance service", provenanceURI);
            provenanceId = provenanceServiceClient.addDocument(token, provenanceURI, null, null);
            logger.info("Registered {} with provenance service with ID: {}", provenanceURI, provenanceId);
        } catch (ProvenanceParentDocumentNotFoundException | ProvenanceAgeOffRuleNotFoundException
                | EzSecurityTokenException | ProvenanceDocumentExistsException
                | ProvenanceCircularInheritanceNotAllowedException e) {
            final String errMsg = "Provenance error";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } catch (final TException e) {
            final String errMsg = "Thrift Pool error";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } finally {
            if (provenanceServiceClient != null) {
                pool.returnToPool(provenanceServiceClient);
            }
        }

        return provenanceId;
    }

    /**
     * Pause generation.
     */
    private void pause() {
        if (pauseMilliseconds > 0) {
            try {
                Thread.sleep(pauseMilliseconds);
            } catch (final InterruptedException e) {
                logger.debug("Processing pause was interrupted");
            }
        }
    }
}
