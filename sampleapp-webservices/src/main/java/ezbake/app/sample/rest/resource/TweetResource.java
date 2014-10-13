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

package ezbake.app.sample.rest.resource;

import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import static ezbake.app.sample.rest.WebServiceUtils.getSecurityToken;
import static ezbake.app.sample.rest.WebServiceUtils.isValidJsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.thrift.TException;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.app.sample.rest.WebServiceException;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.data.mongo.thrift.EzMongo;
import ezbake.data.mongo.thrift.EzMongoBaseException;
import ezbake.data.mongo.thrift.MongoFindParams;
import ezbake.thrift.ThriftClientPool;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

/**
 * REST endpoint to get and search for Tweets in EzMongo.
 */
@Path("tweets")
@Produces(MediaType.APPLICATION_JSON)
public final class TweetResource {
    private static final Logger logger = LoggerFactory.getLogger(TweetResource.class);

    /**
     * Number of spaces used to indent JSON strings.
     */
    private static final int JSON_INDENT = 2;

    /**
     * Application name.
     */
    private final String appName;

    /**
     * Client pool used to create connections to the EzMongo Thrift service.
     */
    private final ThriftClientPool pool;

    /**
     * The current request.
     */
    @Context
    private HttpServletRequest httpRequest;

    /**
     * Constructor.
     */
    public TweetResource() {
        try {
            final Properties props = new EzConfiguration().getProperties();

            appName = new EzBakeApplicationConfigurationHelper(props).getApplicationName();
            pool = new ThriftClientPool(props);
        } catch (final EzConfigurationLoaderException e) {
            final String errMsg = "Could not read EzBake configuration";
            logger.error(errMsg, e);
            throw new WebApplicationException(
                    e, Response.status(INTERNAL_SERVER_ERROR).entity(errMsg).build());
        }
    }

    /**
     * Returns the JSON string of a Tweet document found by its ID.
     *
     * @param tweetId Tweet ID to find
     * @return JSON string of the Tweet document
     */
    @GET
    @Path("{tweetId}")
    public String getTweet(@PathParam("tweetId") String tweetId) {
        logger.info("Called getTweet() with ID {}", tweetId);

        final List<JSONObject> results = doQuery(String.format("{ \"id_str\": \"%s\" }", tweetId));
        if (results.isEmpty()) {
            final String errMsg = "Could not find Tweet with ID " + tweetId;
            logger.error(errMsg);
            throw new WebServiceException(NOT_FOUND, errMsg);
        } else if (results.size() > 1) {
            final String errMsg = "EzMongo returned multiple documents for ID " + tweetId;
            logger.error(errMsg);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, errMsg);
        }

        try {
            return results.get(0).toString(JSON_INDENT);
        } catch (final JSONException e) {
            final String errMsg = "Could not convert Tweet to JSON string";
            logger.error(errMsg, e);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, errMsg);
        }
    }

    /**
     * Queries the Tweet documents with a MongoDB query.
     *
     * @param jsonQuery JSON MongoDB query
     * @return JSON string of the returned Tweet documents
     */
    @POST
    @Path("query")
    @Consumes(MediaType.APPLICATION_JSON)
    public String queryTweets(String jsonQuery) {
        logger.info("Called queryTweets with query: {}", jsonQuery);

        try {
            return new JSONArray(doQuery(jsonQuery)).toString(JSON_INDENT);
        } catch (final JSONException e) {
            final String errMsg = "Could not convert Tweets to JSON string";
            logger.error(errMsg, e);
            throw new WebServiceException(BAD_REQUEST, errMsg);
        }
    }

    /**
     * Perform the actual query to EzMongo and convert the results into a list of JSON objects (one for each document).
     *
     * @param jsonQuery JSON MongoDB query
     * @return List of JSON objects (one for each document)
     */
    private List<JSONObject> doQuery(String jsonQuery) {
        if (!isValidJsonObject(jsonQuery)) {
            final String errMsg = "Query must be a valid JSON object";
            logger.error(errMsg);
            throw new WebServiceException(BAD_REQUEST, errMsg);
        }

        EzMongo.Client client = null;
        try {
            client = pool.getClient(appName, "ezmongo", EzMongo.Client.class);

            final MongoFindParams findParams = new MongoFindParams();
            findParams.setJsonQuery(jsonQuery);
            final List<String> results = client.find("tweets", findParams, getSecurityToken(httpRequest));
            final List<JSONObject> resultObjs = new ArrayList<>(results.size());
            for (final String resultStr : results) {
                resultObjs.add(new JSONObject(resultStr));
            }

            return resultObjs;
        } catch (final EzSecurityTokenException e) {
            final String errMsg = "Security token error";
            logger.error(errMsg, e);
            throw new WebServiceException(FORBIDDEN, errMsg);
        } catch (final EzMongoBaseException e) {
            final String errMsg = "Could not perform Tweet query";
            logger.error(errMsg, e);
            throw new WebServiceException(BAD_REQUEST, errMsg);
        } catch (final TException e) {
            final String errMsg = "Thrift error occurred when performing EzMongo query";
            logger.error(errMsg, e);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, errMsg);
        } catch (final JSONException e) {
            final String errMsg = "EzMongo returned invalid JSON";
            logger.error(errMsg, e);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, errMsg);
        } finally {
            if (client != null) {
                pool.returnToPool(client);
            }
        }
    }
}
