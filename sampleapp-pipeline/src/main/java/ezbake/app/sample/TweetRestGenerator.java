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

import static ezbake.data.image.frack.utilities.IndexingUtils.bytesToHex;
import static ezbake.data.image.frack.utilities.IndexingUtils.getHash;

import java.awt.image.BufferedImage;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.imageio.ImageIO;

import org.apache.commons.io.FilenameUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.app.sample.util.TweetParserUtils;
import ezbake.services.extractor.imagemetadata.thrift.Image;

import twitter4j.TwitterException;

/**
 * Generates {@link ezbake.app.sample.thrift.Tweet} objects from the Twitter REST API and sends them to workers.
 */
public final class TweetRestGenerator extends TweetGenerator {
    private static final long serialVersionUID = -9211283530812559078L;

    private static final Logger logger = LoggerFactory.getLogger(TweetRestGenerator.class);

    @Override
    protected JSONArray initTweets(Properties props) {
        try {
            return TweetRestHarvester.harvestTweets(props);
        } catch (final IOException e) {
            final String errMsg = "Unable to retrieve Tweets - problem getting Twitter user list from file system.";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } catch (final TwitterException e) {
            final String errMsg = "Unable to retrieve Tweets - problem while connecting/connected to Twitter.";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        } catch (final JSONException e) {
            final String errMsg = "Unable to retrieve Tweets - unable to parse JSON";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    protected Map<String, Image> parseImages(JSONObject tweetJson)
            throws JSONException, IOException, NoSuchAlgorithmException {
        final Map<String, Image> images = new HashMap<>(tweetJson.length());
        for (final String imageURL : TweetParserUtils.getImageUrls(tweetJson)) {
            final Image image = new Image();
            try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                final URL url = new URL(imageURL);
                final BufferedImage bufImage = ImageIO.read(url);
                ImageIO.write(bufImage, FilenameUtils.getExtension(url.getFile()), baos);
                image.setBlob(baos.toByteArray());
            }

            image.setOriginalDocumentUri(imageURL);
            image.setFileName(FilenameUtils.getName(imageURL));

            images.put(bytesToHex(getHash(image.getBlob(), image.getFileName())), image);
        }

        return images;
    }
}
