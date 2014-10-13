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

package ezbake.app.sample.rest;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import ezbake.security.client.EzSecurityTokenWrapper;
import ezbake.security.client.EzbakeSecurityClient;

/**
 * Utility class for common SampleApp webservice operations.
 */
public final class WebServiceUtils {
    /**
     * Do not allow instantiation.
     */
    private WebServiceUtils() {
    }

    /**
     * Provides a security token which can be passed along to a thrift service when making thrift calls.
     *
     * @param httpRequest Http request from which thrift calls that require a security token are being made.
     * @return An EzSecurityTokenWrapper from the passed in request.
     */
    public static EzSecurityTokenWrapper getSecurityToken(HttpServletRequest httpRequest) {
        return (EzSecurityTokenWrapper) httpRequest.getSession().getAttribute(EzbakeSecurityClient.SESSION_TOKEN);
    }

    /**
     * Validates that the given string is a valid JSON object.
     *
     * @param jsonQuery String which should be a valid JSON object
     * @return true if the given string is a valid JSON object, false otherwise
     */
    public static boolean isValidJsonObject(String jsonQuery) {
        if (StringUtils.isEmpty(jsonQuery)) {
            return false;
        }

        // Use JSONObject constructor as a validator and ignore object
        try {
            // noinspection ResultOfObjectAllocationIgnored
            new JSONObject(jsonQuery);
        } catch (final JSONException e) {
            return false;
        }

        return true;
    }
}
