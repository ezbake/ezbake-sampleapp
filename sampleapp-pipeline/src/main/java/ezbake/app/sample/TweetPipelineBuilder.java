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

import static org.slf4j.LoggerFactory.getLogger;

import org.slf4j.Logger;

import ezbake.common.properties.EzProperties;
import ezbake.frack.api.Pipeline;
import ezbake.frack.api.PipelineBuilder;

/**
 * Builds the pipeline to process Tweets.
 */
public final class TweetPipelineBuilder implements PipelineBuilder {
    private static final Logger logger = getLogger(TweetPipelineBuilder.class);

    private static final String GENERATOR_ID = "_generator";
    private static final String TWEET_WORKER_ID = "_tweet_worker";
    private static final String RELATIONSHIP_WORKER_ID = "_relationship_worker";
    private static final String IMAGE_WORKER_ID = "_image_worker";

    /**
     * Property set to select whether to harvest Tweets directly from Twitter, or from a saved file.
     */
    private static final String CONNECT_TO_TWITTER = "twitter.connect";
    private static final boolean CONNECT_TO_TWITTER_DEFAULT = false;

    @Override
    public Pipeline build() {
        final Pipeline pipeline = new Pipeline();
        final String pid = pipeline.getId();
        logger.info("Pipeline ID is {}", pid);

        final EzProperties props = new EzProperties(pipeline.getProperties(), false);
        final boolean useTwitter = props.getBoolean(CONNECT_TO_TWITTER, CONNECT_TO_TWITTER_DEFAULT);

        // Generator
        final String gId = pid + GENERATOR_ID;

        // Workers
        final String tweetWorkerId = pid + TWEET_WORKER_ID;
        final String relationshipWorkerId = pid + RELATIONSHIP_WORKER_ID;
        final String imageWorkerId = pid + IMAGE_WORKER_ID;

        if (useTwitter) {
            logger.info("Loading Twitter data from Twitter REST API endpoint");
            pipeline.addGenerator(gId, new TweetRestGenerator());
        } else {
            logger.info("Loading Twitter data from file system");
            pipeline.addGenerator(gId, new TweetFileGenerator());
        }

        pipeline.addWorker(tweetWorkerId, new TweetWorker());
        pipeline.addConnection(gId, tweetWorkerId);

        pipeline.addWorker(relationshipWorkerId, new RelationshipWorker());
        pipeline.addConnection(gId, relationshipWorkerId);

        pipeline.addWorker(imageWorkerId, new ImageWorker());
        pipeline.addConnection(gId, imageWorkerId);

        return pipeline;
    }
}
