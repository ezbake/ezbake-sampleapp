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
import java.util.Properties;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.app.sample.thrift.Tweet;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.Visibility;
import ezbake.data.mongo.thrift.EzMongo;
import ezbake.data.mongo.thrift.MongoEzbakeDocument;
import ezbake.frack.api.Worker;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.thrift.ThriftClientPool;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

/**
 * Adds Tweets as separate documents into EzMongo.
 */
public final class TweetWorker extends Worker<Tweet> {
    private static final long serialVersionUID = -8430767651918044619L;

    private static final Logger logger = LoggerFactory.getLogger(TweetWorker.class);

    /**
     * Security token used to communicate with EzMongo.
     */
    private EzSecurityToken token;

    /**
     * Pool from which to get the EzMongo client.
     */
    private ThriftClientPool pool;

    /**
     * Security client used to get security token.
     */
    private EzbakeSecurityClient securityClient;

    /**
     * Default constructor.
     */
    public TweetWorker() {
        super(Tweet.class);
    }

    @Override
    public void initialize(Properties properties) {
        super.initialize(properties);

        try {
            pool = new ThriftClientPool(properties);

            securityClient = new EzbakeSecurityClient(properties);
            final String securityId = new EzBakeApplicationConfigurationHelper(properties).getSecurityID();
            token = securityClient.fetchAppToken(securityId);
        } catch (final EzSecurityTokenException ex) {
            final String errMsg = "EzSecurity token error";
            logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        }
    }

    @Override
    public void cleanup() {
        super.cleanup();

        if (pool != null) {
            pool.close();
        }

        if (securityClient != null) {
            try {
                securityClient.close();
            } catch (final IOException e) {
                logger.error("Could not close security client", e);
            }
        }
    }

    @Override
    public void process(Visibility visibility, Tweet tweet) {
        logger.info("Received tweet: {}", tweet.getId());

        EzMongo.Client ezMongoClient = null;
        final Visibility updatedVisibility = new Visibility(visibility);
        updatedVisibility.advancedMarkings.setId(tweet.getProvenanceId());
        try {
            ezMongoClient = pool.getClient("ezmongo", EzMongo.Client.class);

            final String mongoDocumentId = ezMongoClient
                    .insert("tweets", new MongoEzbakeDocument(tweet.getRawJson(), updatedVisibility), token);

            logger.info("Inserted Tweet with ID '{}' into MongoDB.", mongoDocumentId);
        } catch (final TException e) {
            logger.error("Thrift error", e);
        } finally {
            if (ezMongoClient != null) {
                pool.returnToPool(ezMongoClient);
            }
        }
    }
}
