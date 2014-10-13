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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ezbake.app.sample.thrift.ReferencedTweet;
import ezbake.app.sample.thrift.TwitterUser;

/**
 * Utility methods to parse out information from the JSON from the Twitter API.
 */
public final class TweetParserUtils {
    private static final String MEDIA_TYPE_KEY = "type";
    private static final String PHOTO_TYPE = "photo";
    private static final String EZBAKE_IMAGE_IDS_KEY = "ezbake_image_ids";
    private static final String NULL_AS_STRING = "null";
    private static final String USER_MENTIONS_KEY = "user_mentions";
    private static final String ENTITIES_KEY = "entities";
    private static final String USER_KEY = "user";
    private static final String ID_STR_KEY = "id_str";
    private static final String PROVENANCE_KEY = "provenance_id";
    private static final String SOURCE_KEY = "source";

    /**
     * Do not allow instantiation.
     */
    private TweetParserUtils() {
    }

    /**
     * Returns the globally unique Tweet ID for the given Tweet.
     *
     * @param tweetJson JSON of the Tweet
     * @return Globally unique Tweet ID
     * @throws JSONException if the Tweet ID could not be retrieved or parsed
     */
    public static String getId(JSONObject tweetJson) throws JSONException {
        return tweetJson.getString(ID_STR_KEY);
    }

    /**
     * Returns the name of the program that created the given Tweet.
     *
     * @param tweetJson JSON of the Tweet
     * @return Source program name
     * @throws JSONException if the source program name could be retrieved
     */
    public static String getSourceProgramName(JSONObject tweetJson) throws JSONException {
        final String sourceProgram = tweetJson.getString(SOURCE_KEY);

        // The name of the source program (of form "<a href="uri">sourceProgramName</a>")
        String sourceProgramName = StringUtils.substringBetween(sourceProgram, ">", "</a>");
        if (sourceProgramName == null) {
            sourceProgramName = sourceProgram;
        }

        // Replace multiple spaces with one (for malformed program names)
        return sourceProgramName.replaceAll("\\s+", " ");
    }

    /**
     * Returns the user info of the author (sender) of the given Tweet. Note that this will not be the original
     * sender/author if the given Tweet was retweeted.
     *
     * @param tweetJson JSON of the Tweet
     * @return Twitter user who sent the Tweet
     * @throws JSONException if the Tweet author could not be retrieved or parsed
     */
    public static TwitterUser getAuthor(JSONObject tweetJson) throws JSONException {
        return getUserInfo(tweetJson.getJSONObject(USER_KEY));
    }

    /**
     * Returns the info for any Twitter users mentioned in the given Tweet.
     *
     * @param tweetJson JSON of the Tweet
     * @return List of mentioned Twitter users
     * @throws JSONException if the user mentions could not be parsed
     */
    public static List<TwitterUser> getMentionedUsers(JSONObject tweetJson) throws JSONException {
        final JSONObject entities = tweetJson.optJSONObject(ENTITIES_KEY);
        if (entities == null) {
            return Collections.emptyList();
        }

        final JSONArray userMentions = entities.optJSONArray(USER_MENTIONS_KEY);
        if (userMentions == null || userMentions.length() == 0) {
            return Collections.emptyList();
        }

        final List<TwitterUser> mentionedUsers = new ArrayList<>(userMentions.length());
        for (int idx = 0; idx < userMentions.length(); idx++) {
            final JSONObject userMention = userMentions.getJSONObject(idx);
            mentionedUsers.add(getUserInfo(userMention));
        }

        return mentionedUsers;
    }

    /**
     * Returns the Twitter-generated image IDs that may be contained in the Tweet.
     *
     * @param tweetJson JSON of the Tweet
     * @return List of Twitter-generated image IDs
     * @throws JSONException if the image IDs could not be parsed
     */
    public static List<String> getTwitterImageIds(JSONObject tweetJson) throws JSONException {
        final List<TwitterPhoto> photos = getPhotos(tweetJson);
        if (photos.isEmpty()) {
            return Collections.emptyList();
        }

        final List<String> imageIds = new ArrayList<>(photos.size());
        for (final TwitterPhoto photo : photos) {
            imageIds.add(photo.getId());
        }

        return imageIds;
    }

    /**
     * Returns the URLs of images that may be contained in the Tweet.
     *
     * @param tweetJson JSON of the Tweet
     * @return List of Twitter-generated image URLs from which the image can be accessed.
     * @throws JSONException if the image URLs could not be parsed
     */
    public static List<String> getImageUrls(JSONObject tweetJson) throws JSONException {
        final List<TwitterPhoto> photos = getPhotos(tweetJson);
        if (photos.isEmpty()) {
            return Collections.emptyList();
        }

        final List<String> imageUrls = new ArrayList<>(photos.size());
        for (final TwitterPhoto photo : photos) {
            imageUrls.add(photo.getMediaUrl());
        }

        return imageUrls;
    }

    /**
     * Returns info for the Tweet retweeted by the given Tweet.
     *
     * @param tweetJson JSON of the Tweet
     * @return {@link ReferencedTweet} or {@code null} if given Tweet is not a retweet
     * @throws JSONException if the retweet info could not be parsed
     */
    public static ReferencedTweet getRetweeted(JSONObject tweetJson) throws JSONException {
        final JSONObject retweetedStatus = tweetJson.optJSONObject("retweeted_status");
        if (retweetedStatus == null) {
            // Not a retweet
            return null;
        }

        return new ReferencedTweet(
                retweetedStatus.getString(ID_STR_KEY), getUserInfo(retweetedStatus.getJSONObject(USER_KEY)));
    }

    /**
     * Returns info for the Tweet to which the given Tweet was a reply.
     *
     * @param tweetJson JSON of the Tweet
     * @return {@link ReferencedTweet} or {@code null} if given Tweet is not a reply
     * @throws JSONException if the reply info could not be parsed
     */
    public static ReferencedTweet getRepliedTo(JSONObject tweetJson) throws JSONException {
        final String tweetId = tweetJson.getString("in_reply_to_status_id_str");
        final String userId = tweetJson.getString("in_reply_to_user_id_str");
        final String userScreenName = tweetJson.getString("in_reply_to_screen_name");

        if (StringUtils.isEmpty(tweetId) || StringUtils.isEmpty(userId) || StringUtils.isEmpty(userScreenName)) {
            // Not a reply
            return null;
        }

        if (tweetId.equals(NULL_AS_STRING) || userId.equals(NULL_AS_STRING) || userScreenName.equals(NULL_AS_STRING)) {
            // Not a reply
            return null;
        }

        return new ReferencedTweet(tweetId, new TwitterUser(userId, userScreenName));
    }

    /**
     * Returns the EzBake-generated image IDs that may be contained in the modified Tweet JSON.
     *
     * @param tweetJson JSON of the Tweet
     * @return List of EzBake-generated image IDs
     * @throws JSONException if the image IDs could not be parsed
     */
    public static List<String> getEzBakeImageIds(JSONObject tweetJson) throws JSONException {
        final JSONArray ezbakeImageIds = tweetJson.optJSONArray(EZBAKE_IMAGE_IDS_KEY);
        if (ezbakeImageIds == null || ezbakeImageIds.length() == 0) {
            return Collections.emptyList();
        }

        final List<String> imageIds = new ArrayList<>(ezbakeImageIds.length());
        for (int idx = 0; idx < ezbakeImageIds.length(); idx++) {
            imageIds.add(ezbakeImageIds.getString(idx));
        }

        return imageIds;
    }

    /**
     * Adds EzBake-generated image IDs to the given JSON object for the Tweet.
     *
     * @param tweetJson JSON object into which to add the image IDs
     * @param ezbakeImageIds EzBake-generated image IDs
     * @throws JSONException if the image IDs could not be added
     */
    public static void setEzBakeImageIds(JSONObject tweetJson, Collection<String> ezbakeImageIds) throws JSONException {
        tweetJson.put(EZBAKE_IMAGE_IDS_KEY, ezbakeImageIds);
    }

    /**
     * Checks if a Tweet has image IDs associated with it.
     *
     * @param tweetJson JSON object to check for image IDs
     * @return true if it has image IDs, false if it doesn't
     * @throws JSONException if the image information could be be retrieved or parsed
     */
    public static boolean hasImages(JSONObject tweetJson) throws JSONException {
        return !getPhotos(tweetJson).isEmpty();
    }

    /**
     * Gets Provenance ID from Tweet.
     *
     * @param tweetJson JSON object containing Tweet and Provenance ID
     * @return Provenance ID stored with Tweet, returns -1 if not set
     * @throws JSONException if getLong fails
     */
    public static long getEzBakeProvenanceId(JSONObject tweetJson) throws JSONException {
        return tweetJson.getLong(PROVENANCE_KEY);
    }

    /**
     * Sets Provenance ID to Tweet.
     *
     * @param tweetJson JSON object containing Tweet
     * @param provenanceId Provenance ID to store with Tweet
     * @throws JSONException if put fails
     */
    public static void setEzBakeProvenanceId(JSONObject tweetJson, long provenanceId) throws JSONException {
        tweetJson.put(PROVENANCE_KEY, provenanceId);
    }

    /**
     * Creates a {@link TwitterUser} from a JSON object containing user info.
     *
     * @param userObj JSON object containing user info
     * @return {@link TwitterUser} version of the info contained in JSON
     * @throws JSONException if the required user information could not be retrieved or parsed
     */
    private static TwitterUser getUserInfo(JSONObject userObj) throws JSONException {
        final TwitterUser user = new TwitterUser();
        user.setId(userObj.getString(ID_STR_KEY));
        user.setScreenName(userObj.getString("screen_name"));

        final String name = userObj.optString("name");
        if (!StringUtils.isEmpty(name)) {
            user.setName(name);
        }

        return user;
    }

    /**
     * Checks the JSON of a Tweet for photo entries and returns them in a list.
     *
     * @param tweetJson The Tweet to parse
     * @return Returns a List<TwitterPhoto> populated by the photo entries from a Tweet.
     * @throws JSONException if there was an issue parsing the photos out
     */
    private static List<TwitterPhoto> getPhotos(JSONObject tweetJson) throws JSONException {
        final JSONObject entities = tweetJson.optJSONObject(ENTITIES_KEY);
        if (entities == null) {
            return Collections.emptyList();
        }

        final JSONArray media = entities.optJSONArray("media");
        if (media == null || media.length() == 0) {
            return Collections.emptyList();
        }

        final List<TwitterPhoto> photos = new ArrayList<>();
        for (int idx = 0; idx < media.length(); idx++) {
            final JSONObject mediaObject = media.getJSONObject(idx);

            if (mediaObject.getString(MEDIA_TYPE_KEY).equals(PHOTO_TYPE)) {
                final TwitterPhoto tp = new TwitterPhoto();
                tp.setId(mediaObject.getString(ID_STR_KEY));
                tp.setMediaUrl(mediaObject.getString("media_url"));
                photos.add(tp);
            }
        }

        return photos;
    }

    /**
     * POJO for storing 'photo' properties from parsed Tweet JSON.
     */
    private static final class TwitterPhoto {
        /**
         * Photo ID.
         */
        private String id;

        /**
         * URL for photo file (for download).
         */
        private String mediaUrl;

        /**
         * Get photo ID.
         *
         * @return Photo ID
         */
        public String getId() {
            return id;
        }

        /**
         * Set photo ID.
         *
         * @param id New photo ID
         */
        public void setId(String id) {
            this.id = id;
        }

        /**
         * Get URL for photo file.
         *
         * @return URL for photo file
         */
        public String getMediaUrl() {
            return mediaUrl;
        }

        /**
         * Set media URL for photo file.
         *
         * @param mediaUrl New media URL
         */
        public void setMediaUrl(String mediaUrl) {
            this.mediaUrl = mediaUrl;
        }
    }
}
