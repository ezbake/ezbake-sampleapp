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
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import static ezbake.app.sample.rest.WebServiceUtils.getSecurityToken;

import java.util.List;

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
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Range;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import ezbake.app.sample.rest.WebServiceException;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.common.properties.EzProperties;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.data.common.graph.GraphConverter;
import ezbake.services.graph.thrift.EzGraphService;
import ezbake.services.graph.thrift.EzGraphServiceConstants;
import ezbake.services.graph.thrift.GraphName;
import ezbake.services.graph.thrift.types.Edge;
import ezbake.services.graph.thrift.types.Graph;
import ezbake.services.graph.thrift.types.Vertex;
import ezbake.thrift.ThriftClientPool;

/**
 * REST endpoints for accessing data stored via the EzGraph Service.
 */
@Path("relationships")
public final class RelationshipResource {
    private static final Logger logger = LoggerFactory.getLogger(RelationshipResource.class);

    /**
     * Range for the number of hops for {@link #expandSubGraph(String, int)} methods.
     */
    private static final Range<Integer> EXPAND_HOPS_RANGE = Range.closed(0, 8);

    /**
     * Range for the number of hops for {@link #findPath(String, String, int)}.
     */
    private static final Range<Integer> FIND_HOPS_RANGE = Range.closed(1, 8);

    private static final String THRIFT_ERROR_MSG =
            "An error occurred while accessing a Thrift resource. The thrift service may be unavailable.";

    private static final String USER_NOT_FOUND_MSG = "No user found with name: ";

    private static final String NUMHOPS_ERR_MSG =
            "The final URI segment, specifying the number of hops from the start vertex,"
                    + " must be a valid number from %d to %d. You entered: %d";

    /**
     * Constants for graph setup.
     */
    private static final String SCREEN_NAME = "screenName";
    private static final GraphName GRAPH_NAME = new GraphName().setName("sampleGraph");

    /**
     * Valid Gremlin query types.
     */
    private static final String EDGE = "edge";
    private static final String VERTEX = "vertex";

    private final ThriftClientPool pool;
    private final Gson gson;

    @Context
    private HttpServletRequest httpRequest;

    /**
     * Constructor.
     */
    public RelationshipResource() {
        final EzProperties props;
        try {
            props = new EzProperties(new EzConfiguration().getProperties(), true);
            pool = new ThriftClientPool(props);
            gson = new GsonBuilder().setPrettyPrinting().create();
        } catch (final EzConfigurationLoaderException e) {
            final String errMsg = "Could not read EzBake configuration";
            logger.error(errMsg, e);
            throw new WebApplicationException(
                    e, Response.status(INTERNAL_SERVER_ERROR).entity(errMsg).build());
        }
    }

    /**
     * Gets a single Vertex based on a Twitter user's screen name.
     *
     * @param graphClient The EzGraphService.Client to use to find the Vertex with the associated passed in screen
     * name.
     * @param screenName The screen name whose associated vertex will be searched for.
     * @param token Security token required by thrift calls.
     * @return The Vertex associated with the passed in screen name.
     * @throws TException if there is an error while making the thrift request on the EzGraph client..
     */
    private static Vertex getUserVertex(EzGraphService.Client graphClient, String screenName, EzSecurityToken token)
            throws TException {
        final List<Vertex> foundVertices =
                graphClient.findVertices(GRAPH_NAME, SCREEN_NAME, GraphConverter.convertObject(screenName), token);

        return foundVertices.isEmpty() ? null : foundVertices.get(0);
    }

    /**
     * Endpoint to make Gremlin queries through the EzGraphService on the SampleApp's graph. Currently response
     * information about malformed Gremlin queries is poor. Valid Gremlin queries include: "_().outE()" on an edge query
     * to get all edges connected to the vertex specified by 'screenName' or "_().out("mentioned")" on a vertex query to
     * get all vertices (users) mentioned by the user specified by 'screenName'. These examples are not yet escaped.
     * Query params are passed in the post body.
     *
     * @param jsonQuery A string representation of a JSON object with fields 'type' (valued either 'vertex' or 'edge'),
     * '{@link #SCREEN_NAME}' (valued any Twitter screen name expected to be stored in graph) and 'gremlin' (valued at
     * any valid gremlin query).
     * @return Gremlin query data in JSON form.
     */
    @POST
    @Path("gremlin-query")
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public String queryGraph(String jsonQuery) {
        final JSONObject json;
        final String type;
        final String screenName;
        final String gremlinQuery;
        try {
            json = new JSONObject(jsonQuery);

            type = json.getString("type");
            screenName = json.getString(SCREEN_NAME);
            gremlinQuery = json.getString("gremlin");
        } catch (final JSONException e) {
            final String errMsg = "Could not parse JSON in request";
            logger.error(errMsg, e);
            throw new WebServiceException(BAD_REQUEST, errMsg);
        }

        EzGraphService.Client graphClient = null;
        try {
            final EzSecurityToken token = getSecurityToken(httpRequest);
            graphClient = getGraphDbClient();

            final Vertex startVertex = getUserVertex(graphClient, screenName, token);
            if (startVertex == null) {
                throw new WebServiceException(NOT_FOUND, USER_NOT_FOUND_MSG + screenName);
            }

            switch (type) {
                case VERTEX:
                    final List<Vertex> vertexResults =
                            graphClient.queryVertices(GRAPH_NAME, startVertex, gremlinQuery, token);

                    return gson.toJson(vertexResults);
                case EDGE:
                    final List<Edge> edgeResults = graphClient.queryEdges(GRAPH_NAME, startVertex, gremlinQuery, token);
                    return gson.toJson(edgeResults);
                default:
                    final String errMsg =
                            String.format("Invalid query: 'type' must be either '%s' or '%s'", EDGE, VERTEX);

                    throw new WebServiceException(BAD_REQUEST, errMsg);
            }
        } catch (final TException e) {
            logger.error(
                    "Thrift error when attempting to perform Gremlin query. screenName={} type={} query={}.",
                    screenName, type, gremlinQuery, e);

            throw new WebServiceException(
                    INTERNAL_SERVER_ERROR,
                    "An error occurred while accessing a thrift resource. Issues that could cause this include the "
                            + "Thrift resource being unavailable, a malformed Gremlin query, or the query attempted "
                            + "did not return the type expected ('vertex' or 'edge').");
        } finally {
            if (graphClient != null) {
                pool.returnToPool(graphClient);
            }
        }
    }

    /**
     * Gets a graph with a specific vertex as the start point expanded a certain number of hops.
     *
     * @param screenName The name of the user used to find the start vertex for the expanded graph.
     * @param numHops The number of hops to expand the graph, up to a specified maximum.
     * @return A graph with the vertices and edges 'numHops' from the start vertex.
     */
    @GET
    @Path("expand/{screenName}/{numHops}")
    @Produces(MediaType.APPLICATION_JSON)
    public String expandSubGraph(@PathParam(SCREEN_NAME) String screenName, @PathParam("numHops") int numHops) {
        if (!EXPAND_HOPS_RANGE.contains(numHops)) {
            final String errMsg = String.format(
                    NUMHOPS_ERR_MSG, EXPAND_HOPS_RANGE.lowerEndpoint(), EXPAND_HOPS_RANGE.upperEndpoint(), numHops);

            throw new WebServiceException(BAD_REQUEST, errMsg);
        }

        final EzGraphService.Client graphClient;
        try {
            final EzSecurityToken token = getSecurityToken(httpRequest);
            graphClient = getGraphDbClient();

            final Vertex startVertex = getUserVertex(graphClient, screenName, token);
            if (startVertex == null) {
                throw new WebServiceException(NOT_FOUND, USER_NOT_FOUND_MSG + screenName);
            }

            final Graph graph = graphClient.expandSubgraph(GRAPH_NAME, startVertex, numHops, token);
            return gson.toJson(graph);
        } catch (final TException e) {
            logger.error(THRIFT_ERROR_MSG, e);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, THRIFT_ERROR_MSG);
        }
    }

    /**
     * Finds the path between two vertices up to a specified number of hops away.
     *
     * @param startScreenName The user from which to find a path.
     * @param endScreenName The user to find a path to.
     * @param maxHops The maximum number of hops that can be searched to connect users.
     * @return The graph of the path between two users' vertices.
     */
    @GET
    @Path("find/{screenName}/{endScreenName}/{maxHops}")
    @Produces(MediaType.APPLICATION_JSON)
    public String findPath(
            @PathParam(SCREEN_NAME) String startScreenName, @PathParam("endScreenName") String endScreenName,
            @PathParam("maxHops") int maxHops) {
        if (!FIND_HOPS_RANGE.contains(maxHops)) {
            final String errMsg = String.format(
                    NUMHOPS_ERR_MSG, FIND_HOPS_RANGE.lowerEndpoint(), FIND_HOPS_RANGE.upperEndpoint(), maxHops);

            throw new WebServiceException(BAD_REQUEST, errMsg);
        }

        final EzGraphService.Client graphClient;
        try {
            final EzSecurityToken token = getSecurityToken(httpRequest);
            graphClient = getGraphDbClient();

            final Vertex startVertex = getUserVertex(graphClient, startScreenName, token);
            final Vertex endVertex = getUserVertex(graphClient, endScreenName, token);

            if (startVertex == null) {
                throw new WebServiceException(NOT_FOUND, USER_NOT_FOUND_MSG + startScreenName);
            }

            if (endVertex == null) {
                throw new WebServiceException(NOT_FOUND, USER_NOT_FOUND_MSG + endScreenName);
            }

            final Graph graph = graphClient.findPath(GRAPH_NAME, startVertex, endVertex, maxHops, token);
            return gson.toJson(graph);
        } catch (final TException e) {
            logger.error(THRIFT_ERROR_MSG, e);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, THRIFT_ERROR_MSG);
        }
    }

    /**
     * Gets the graph client for the specified resource.
     *
     * @return An EzGraphService.Client on which thrift requests for graph data can be made.
     * @throws TException if there is a problem retrieving the client from the pool.
     */
    private EzGraphService.Client getGraphDbClient() throws TException {
        return pool.getClient(EzGraphServiceConstants.SERVICE_NAME, EzGraphService.Client.class);
    }
}
