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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.app.sample.util.TweetParserUtils;
import ezbake.common.properties.EzProperties;
import ezbake.services.extractor.imagemetadata.thrift.Image;

/**
 * Generates {@link ezbake.app.sample.thrift.Tweet} objects from a JSON file and sends them to workers.
 */
public final class TweetFileGenerator extends TweetGenerator {
    private static final long serialVersionUID = -1218280117602790346L;

    private static final String TWEETS_JSON_FILE_PROP = "tweets.json.file";
    private static final String TWEETS_IMAGE_DIR_PROP = "tweets.image.dir";

    private static final Logger logger = LoggerFactory.getLogger(TweetFileGenerator.class);

    private Path tweetImageDir;

    @Override
    protected JSONArray initTweets(Properties props) {
        final EzProperties config = new EzProperties(props, false);

        tweetImageDir = Paths.get(config.getProperty(TWEETS_IMAGE_DIR_PROP));

        try {
            final String tweetsJsonPath = config.getProperty(TWEETS_JSON_FILE_PROP);
            return new JSONArray(IOUtils.toString(new File(tweetsJsonPath).toURI(), "UTF-8"));
        } catch (SecurityException | IllegalArgumentException | JSONException | IOException e) {
            final String errMsg = "Could not read and parse JSON tweets file";
            logger.error(errMsg, e);
            throw new RuntimeException(errMsg, e);
        }
    }

    @Override
    protected Map<String, Image> parseImages(JSONObject tweetJson)
            throws JSONException, IOException, NoSuchAlgorithmException {
        final Map<String, Image> images = new HashMap<>(tweetJson.length());
        for (final String twitterImageId : TweetParserUtils.getTwitterImageIds(tweetJson)) {
            final File[] matchingImages = tweetImageDir.toFile().listFiles(
                    new FilenameFilter() {
                        @Override
                        public boolean accept(File dir, String name) {
                            return name.startsWith(twitterImageId + '.');
                        }
                    });

            if (matchingImages == null || matchingImages.length == 0) {
                throw new IOException(String.format("Could not read image '%s' referenced from Tweet", twitterImageId));
            } else if (matchingImages.length > 1) {
                throw new IOException(String.format("Found multiple images with image with ID '%s'", twitterImageId));
            }

            final Path imagePath = matchingImages[0].toPath();

            final Image image = new Image();
            image.setBlob(Files.readAllBytes(imagePath));
            image.setOriginalDocumentUri(imagePath.toUri().toString());
            image.setFileName(imagePath.getFileName().toString());

            images.put(bytesToHex(getHash(image.getBlob(), image.getFileName())), image);
        }

        return images;
    }
}
