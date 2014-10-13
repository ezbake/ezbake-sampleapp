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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

import ezbake.app.sample.thrift.Tweet;
import ezbake.app.sample.util.SampleAppConstants;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.Visibility;
import ezbake.frack.api.Worker;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.extractor.imagemetadata.thrift.Image;
import ezbake.services.indexing.image.thrift.Document;
import ezbake.services.indexing.image.thrift.ImageIndexerService;
import ezbake.services.indexing.image.thrift.ImageIndexerServiceConstants;
import ezbake.services.indexing.image.thrift.IngestedDocumentInfo;
import ezbake.services.indexing.image.thrift.IngestedImageInfo;
import ezbake.services.provenance.thrift.InheritanceInfo;
import ezbake.services.provenance.thrift.ProvenanceService;
import ezbake.thrift.ThriftClientPool;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

/**
 * Adds images from Tweets into image indexer service for metadata and similarity searches.
 */
public final class ImageWorker extends Worker<Tweet> {
    private static final long serialVersionUID = -2301920607130063078L;

    private static final Logger logger = LoggerFactory.getLogger(ImageWorker.class);

    /**
     * Security token used to communicate with image indexer service.
     */
    private EzSecurityToken token;

    /**
     * Pool from which to get Thrift clients.
     */
    private ThriftClientPool pool;

    /**
     * Security client used to get security token.
     */
    private EzbakeSecurityClient securityClient;

    /**
     * Default constructor.
     */
    public ImageWorker() {
        super(Tweet.class);
    }

    /**
     * Generate Inheritance Information from a Tweet ID.
     *
     * @param tweetId ID of parent object (Tweet)
     * @return InheritanceInfo object
     */
    private static List<InheritanceInfo> getInheritanceInfoFromTweetId(String tweetId) {
        final InheritanceInfo inheritanceInfo =
                new InheritanceInfo().setParentUri(SampleAppConstants.getTweetUri(tweetId));

        return Collections.singletonList(inheritanceInfo);
    }

    @Override
    public void initialize(Properties props) {
        super.initialize(props);

        try {
            pool = new ThriftClientPool(props);

            securityClient = new EzbakeSecurityClient(props);
            final String securityId = new EzBakeApplicationConfigurationHelper(props).getSecurityID();
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
        if (tweet.getImagesSize() == 0) {
            logger.info("Tweet {} has no images. Skipping processing", tweet.getId());
            return;
        }

        logger.info("Processing images from tweet {}", tweet.getId());

        ImageIndexerService.Client imageIndexer = null;
        ProvenanceService.Client provenanceServiceClient = null;
        try {
            imageIndexer = pool.getClient(ImageIndexerServiceConstants.SERVICE_NAME, ImageIndexerService.Client.class);

            provenanceServiceClient = pool.getClient("EzProvenanceService", ProvenanceService.Client.class);

            final List<Document> imageDocs = new ArrayList<>(tweet.getImagesSize());
            for (final Entry<String, Image> imageEntry : tweet.getImages().entrySet()) {
                final Image image = imageEntry.getValue();
                final Document imageDoc = new Document();
                final Visibility updatedVisibility = new Visibility(visibility);
                final String imageURI = SampleAppConstants.getImageUri(image.getFileName());
                final List<InheritanceInfo> inheritanceInfo = getInheritanceInfoFromTweetId(tweet.getId());
                logger.info("Registering {} with provenance service, inheritance info: {}", imageURI, inheritanceInfo);
                final long provenanceId = provenanceServiceClient.addDocument(token, imageURI, inheritanceInfo, null);

                logger.info("Registered with provenance service with ID: {}", provenanceId);

                updatedVisibility.getAdvancedMarkings().setId(provenanceId);

                imageDoc.setBlob(image.getBlob());
                imageDoc.setFileName(image.getFileName());
                imageDoc.setVisibility(updatedVisibility);

                imageDocs.add(imageDoc);
            }

            final List<IngestedDocumentInfo> ingestedDocsInfo = imageIndexer.ingestDocuments(imageDocs, token);

            final Set<String> ingestedIds = new HashSet<>(ingestedDocsInfo.size());
            for (final IngestedDocumentInfo ingestedDocInfo : ingestedDocsInfo) {
                final List<IngestedImageInfo> ingestedImages = ingestedDocInfo.getIngestedImages();
                if (!ingestedImages.isEmpty()) {
                    ingestedIds.add(ingestedImages.get(0).getImageId());
                }
            }

            final Set<String> expectedIds = tweet.getImages().keySet();
            if (!ingestedIds.equals(expectedIds)) {
                final String errMsg = String.format(
                        "Tweet '%s' expected to have EzBake image IDs %s but instead contained %s with the "
                                + "difference of %s", tweet.getId(), expectedIds, ingestedIds,
                        Sets.symmetricDifference(expectedIds, ingestedIds));

                logger.error(errMsg);
                throw new IllegalStateException(errMsg);
            }

            logger.info("Tweet '{}' has images with EzBake IDs of {}", tweet.getId(), ingestedIds);
        } catch (final TException e) {
            logger.error("Thrift error", e);
        } finally {
            if (imageIndexer != null) {
                pool.returnToPool(imageIndexer);
            }
            if (provenanceServiceClient != null) {
                pool.returnToPool(provenanceServiceClient);
            }
        }
    }
}
