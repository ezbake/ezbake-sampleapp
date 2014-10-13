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

import org.junit.Test;

/**
 * Unit tests for {@link SampleAppConstants}.
 */
public final class SampleAppConstantsTest {
    private static final String TEST_ID = "test";

    /**
     * Tests that {@link SampleAppConstants#getTweetUri(String)} returns the expected output.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testGetTweetUri() throws Exception {
        final String tweetUri = SampleAppConstants.getTweetUri(TEST_ID);
        assertEquals(SampleAppConstants.TWEET_URI + TEST_ID, tweetUri);
    }

    /**
     * Tests that {@link SampleAppConstants#getImageUri(String)} returns the expected output.
     *
     * @throws Exception if an error occurred
     */
    @Test
    public void testGetImageUri() throws Exception {
        final String tweetUri = SampleAppConstants.getImageUri(TEST_ID);
        assertEquals(SampleAppConstants.IMAGE_URI + TEST_ID, tweetUri);
    }
}
