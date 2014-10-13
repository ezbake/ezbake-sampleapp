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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import ezbake.app.sample.thrift.ReferencedTweet;
import ezbake.app.sample.thrift.TwitterUser;

/**
 * Unit tests for {@link TweetParserUtils}.
 */
@SuppressWarnings({"checkstyle:multiplestringliterals", "StaticNonFinalField"})
public final class TweetParserUtilsTest {
    /**
     * Tweets read in from test JSON file.
     */
    private static JSONArray tweets = new JSONArray();

    /**
     * Reads in Tweets from JSON test file.
     *
     * @throws Exception if an error occurred
     */
    @BeforeClass
    public static void setupClass() throws Exception {
        try (InputStream in = TweetParserUtilsTest.class.getResourceAsStream("/tweets.json")) {
            tweets = new JSONArray(IOUtils.toString(in));
        }
    }

    /**
     * Tests that Tweet IDs can be parsed.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testId() throws Exception {
        final List<String> expected = new ArrayList<>(tweets.length());
        expected.add("436206756168220674");
        expected.add("436206756189200384");
        expected.add("436206756201779200");
        expected.add("436206756176601088");
        expected.add("436206756193386496");
        expected.add("436206756168220672");
        expected.add("436206756176613376");
        expected.add("436206756172402688");
        expected.add("436206756201775104");
        expected.add("436206756176613377");
        expected.add("436206756188794880");
        expected.add("436206756201783296");
        expected.add("436206756197183488");
        expected.add("436206756168216576");
        expected.add("436206764582002688");
        expected.add("436206756168228864");
        expected.add("436206756205977600");

        final List<String> actual = new ArrayList<>(tweets.length());
        for (int idx = 0; idx < tweets.length(); idx++) {
            actual.add(TweetParserUtils.getId(tweets.getJSONObject(idx)));
        }

        assertEquals(expected, actual);
    }

    /**
     * Tests that the source program of a Tweet can be retrieved and parsed.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testSourceProgramName() throws Exception {
        final List<String> expected = new ArrayList<>(tweets.length());
        expected.add("web");
        expected.add("Twitter for Android");
        expected.add("Twitter for Android");
        expected.add("Twitter for Android");
        expected.add("Twitter for iPad");
        expected.add("Twitter for iPhone");
        expected.add("Twitter for iPhone");
        expected.add("Twitter for Android");
        expected.add("Twitter for Android");
        expected.add("S1Gateway");
        expected.add("Twitter for Android");
        expected.add("Twitter for iPhone");
        expected.add("Twitter for iPhone");
        expected.add("web");
        expected.add("web");
        expected.add("Twitter for iPhone");
        expected.add("Twitter for Android");

        final List<String> actual = new ArrayList<>(tweets.length());
        for (int idx = 0; idx < tweets.length(); idx++) {
            actual.add(TweetParserUtils.getSourceProgramName(tweets.getJSONObject(idx)));
        }

        assertEquals(expected, actual);
    }

    /**
     * Tests that the author of a Tweet can be parsed.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testAuthorship() throws Exception {
        final List<TwitterUser> expected = new ArrayList<>(tweets.length());
        expected.add(new TwitterUser("1497050233", "acidwashedwifi").setName("\u2022"));
        expected.add(new TwitterUser("2207766096", "Touneeeeeees").setName("NoTweetNameWitch"));
        expected.add(new TwitterUser("470163461", "reasonsbiebers").setName("      tai"));
        expected.add(new TwitterUser("2257232182", "NourhanNFarid").setName("Nourhan Naim"));
        expected.add(new TwitterUser("219971799", "thomashauer_at").setName("Thomas Hauer"));
        expected.add(new TwitterUser("493323790", "polaroidliam").setName("leonie \u273f"));
        expected.add(new TwitterUser("200986842", "ALWAYZsayALWAYZ").setName("\u265b"));
        expected.add(new TwitterUser("221003445", "perryhoff").setName("Perry Hoffman"));
        expected.add(new TwitterUser("371593292", "lilOba_").setName("#48"));
        expected.add(new TwitterUser("130893902", "ClaroArgentina").setName("Claro Argentina"));
        expected.add(new TwitterUser("526996626", "DalenAnderson").setName("The D\u2122"));
        expected.add(new TwitterUser("142996411", "Quibu5").setName("#ItAintRalphThough !"));
        expected.add(new TwitterUser("263873739", "Lacey__Stevens").setName("The Lacey Stevens \u2020"));
        expected.add(new TwitterUser("1563833244", "uaijusten_").setName("Rachel"));
        expected.add(new TwitterUser("1596692250", "danielsj97").setName("Daniels"));
        expected.add(new TwitterUser("576219676", "isito21").setName("lisito"));
        expected.add(new TwitterUser("453332322", "Jaimesitto").setName("Original Jaimeman"));

        final List<TwitterUser> actual = new ArrayList<>(tweets.length());
        for (int idx = 0; idx < tweets.length(); idx++) {
            actual.add(TweetParserUtils.getAuthor(tweets.getJSONObject(idx)));
        }

        assertEquals(expected, actual);
    }

    /**
     * Tests that users that were mentioned in the Tweet can be parsed.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testMentionedUsers() throws Exception {
        final List<List<TwitterUser>> expected = new ArrayList<>(tweets.length());
        expected.add(ImmutableList.of(new TwitterUser("1353275904", "starfishlilo").setName("slayvanna")));
        expected.add(ImmutableList.of(new TwitterUser("2179551148", "OhEscobar")));

        expected.add(
                ImmutableList.of(new TwitterUser("377641061", "lightsjusten").setName("amanda n\u00e3o seyfried")));

        expected.add(
                ImmutableList.of(
                        new TwitterUser("159512330", "MmaQarat")
                                .setName("\u0645\u0645\u0627 \u0642\u0631\u0623\u062a")));

        expected.add(ImmutableList.of(new TwitterUser("398992020", "SoheeFit").setName("Sohee Lee")));
        expected.add(new ArrayList<TwitterUser>());
        expected.add(new ArrayList<TwitterUser>());
        expected.add(ImmutableList.of(new TwitterUser("27804980", "ecojustice_ca").setName("Ecojustice ")));

        expected.add(
                ImmutableList.of(
                        new TwitterUser("166468479", "___TrippyHippy").setName("UGK HB."),
                        new TwitterUser("371593292", "lilOba_").setName("#48")));

        expected.add(ImmutableList.of(new TwitterUser("15973550", "scriado").setName("Sebasti\u00e1n D. Criado")));
        expected.add(ImmutableList.of(new TwitterUser("537072912", "jordancolbert22").setName("black sheep")));
        expected.add(ImmutableList.of(new TwitterUser("394247092", "_WhitneyLove_").setName("Jane Doe")));
        expected.add(ImmutableList.of(new TwitterUser("370428124", "rapIikelilwayne").setName("rap like lil wayne")));
        expected.add(ImmutableList.of(new TwitterUser("285746672", "3dconcert").setName("bruna")));
        expected.add(ImmutableList.of(new TwitterUser("1598587332", "Sporta_Pasaule").setName("Sporta Pasaule")));
        expected.add(new ArrayList<TwitterUser>());
        expected.add(new ArrayList<TwitterUser>());

        final List<List<TwitterUser>> actual = new ArrayList<>(tweets.length());
        for (int idx = 0; idx < tweets.length(); idx++) {
            actual.add(TweetParserUtils.getMentionedUsers(tweets.getJSONObject(idx)));
        }

        assertEquals(expected, actual);
    }

    /**
     * Tests that Twitter IDs for images in the Tweet can be parsed.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testTwitterImageIds() throws Exception {
        final List<List<String>> expected = new ArrayList<>(tweets.length());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(ImmutableList.of("436206755941732353"));
        expected.add(ImmutableList.of("436206756050763776"));

        final List<List<String>> actual = new ArrayList<>(tweets.length());
        for (int idx = 0; idx < tweets.length(); idx++) {
            actual.add(TweetParserUtils.getTwitterImageIds(tweets.getJSONObject(idx)));
        }

        assertEquals(expected, actual);
    }

    /**
     * Tests that URLs of image files in the Tweet can be parsed.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testImageURLs() throws Exception {
        final List<List<String>> expected = new ArrayList<>(tweets.length());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(new ArrayList<String>());
        expected.add(ImmutableList.of("http://pbs.twimg.com/media/Bg23u8CIUAEQhnh.jpg"));
        expected.add(ImmutableList.of("http://pbs.twimg.com/media/Bg23u8cIAAAyYh8.png"));

        final List<List<String>> actual = new ArrayList<>(tweets.length());
        for (int idx = 0; idx < tweets.length(); idx++) {
            actual.add(TweetParserUtils.getImageUrls(tweets.getJSONObject(idx)));
        }

        assertEquals(expected, actual);
    }

    /**
     * Tests that info of a retweeted Tweet can be parsed.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testRetweeted() throws Exception {
        final List<ReferencedTweet> expected = new ArrayList<>(tweets.length());
        expected.add(null);
        expected.add(new ReferencedTweet("436190409405255680", new TwitterUser("2179551148", "OhEscobar")));
        expected.add(null);

        expected.add(
                new ReferencedTweet(
                        "436203706150039552", new TwitterUser("159512330", "MmaQarat")
                        .setName("\u0645\u0645\u0627 \u0642\u0631\u0623\u062a")));

        expected.add(
                new ReferencedTweet(
                        "436204150066790401", new TwitterUser("398992020", "SoheeFit").setName("Sohee Lee")));

        expected.add(null);
        expected.add(null);

        expected.add(
                new ReferencedTweet(
                        "436201171938971648", new TwitterUser("27804980", "ecojustice_ca").setName("Ecojustice ")));

        expected.add(
                new ReferencedTweet(
                        "436206159650103296", new TwitterUser("166468479", "___TrippyHippy").setName("UGK HB.")));

        expected.add(null);

        expected.add(
                new ReferencedTweet(
                        "436206651218362368", new TwitterUser("537072912", "jordancolbert22").setName("black sheep")));

        expected.add(null);

        expected.add(
                new ReferencedTweet(
                        "432567352841240576",
                        new TwitterUser("370428124", "rapIikelilwayne").setName("rap like lil wayne")));

        expected.add(null);

        expected.add(
                new ReferencedTweet(
                        "436205457158332416",
                        new TwitterUser("1598587332", "Sporta_Pasaule").setName("Sporta Pasaule")));

        expected.add(null);
        expected.add(null);

        final List<ReferencedTweet> actual = new ArrayList<>(tweets.length());
        for (int idx = 0; idx < tweets.length(); idx++) {
            actual.add(TweetParserUtils.getRetweeted(tweets.getJSONObject(idx)));
        }

        assertEquals(expected, actual);
    }

    /**
     * Tests that info of a replied-to Tweet can be parsed.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testRepliedTo() throws Exception {
        final List<ReferencedTweet> expected = new ArrayList<>(tweets.length());
        expected.add(new ReferencedTweet("436206418945798144", new TwitterUser("1353275904", "starfishlilo")));
        expected.add(null);
        expected.add(new ReferencedTweet("436204411472605186", new TwitterUser("377641061", "lightsjusten")));
        expected.add(null);
        expected.add(null);
        expected.add(null);
        expected.add(null);
        expected.add(null);
        expected.add(null);
        expected.add(new ReferencedTweet("436205272244027392", new TwitterUser("15973550", "scriado")));
        expected.add(null);
        expected.add(new ReferencedTweet("436206669396451328", new TwitterUser("394247092", "_WhitneyLove_")));
        expected.add(null);
        expected.add(new ReferencedTweet("436206196106600448", new TwitterUser("285746672", "3dconcert")));
        expected.add(null);
        expected.add(null);
        expected.add(null);

        final List<ReferencedTweet> actual = new ArrayList<>(tweets.length());
        for (int idx = 0; idx < tweets.length(); idx++) {
            actual.add(TweetParserUtils.getRepliedTo(tweets.getJSONObject(idx)));
        }

        assertEquals(expected, actual);
    }

    /**
     * Tests that EzBake image IDs can be added and retrieved from a Tweet JSON object.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testGetSetEzBakeImageIds() throws Exception {
        final List<String> testIds = ImmutableList.of("ABC123", "987FED");
        final JSONObject testObj = new JSONObject();

        assertTrue(TweetParserUtils.getEzBakeImageIds(testObj).isEmpty());

        TweetParserUtils.setEzBakeImageIds(testObj, testIds);
        assertEquals(testIds, TweetParserUtils.getEzBakeImageIds(testObj));
    }

    /**
     * Tests that EzBake provenance IDs can be added and retrieved from a Tweet JSON object.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testGetSetEzBakeProvenanceId() throws Exception {
        final JSONObject testObj = new JSONObject();

        try {
            TweetParserUtils.getEzBakeProvenanceId(testObj);
            fail("Expected exception was not thrown");
        } catch (final JSONException e) {
            // Expected
        }

        final long expectedProvenanceId = 42;
        TweetParserUtils.setEzBakeProvenanceId(testObj, expectedProvenanceId);
        assertEquals(expectedProvenanceId, TweetParserUtils.getEzBakeProvenanceId(testObj));
    }

    /**
     * Tests that hasImages method checks a Tweet JSON object for associated image IDs.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testHasImages() throws Exception {
        final List<Boolean> expected = new ArrayList<>(tweets.length());
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(false);
        expected.add(true);
        expected.add(true);

        final List<Boolean> actual = new ArrayList<>(tweets.length());
        for (int idx = 0; idx < tweets.length(); idx++) {
            actual.add(TweetParserUtils.hasImages(tweets.getJSONObject(idx)));
        }

        assertEquals(expected, actual);
    }
}
