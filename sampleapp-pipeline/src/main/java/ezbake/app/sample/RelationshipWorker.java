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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import ezbake.app.sample.thrift.Tweet;
import ezbake.app.sample.thrift.TwitterUser;
import ezbake.base.thrift.EzSecurityToken;
import ezbake.base.thrift.EzSecurityTokenException;
import ezbake.base.thrift.Visibility;
import ezbake.data.common.graph.GraphConverter;
import ezbake.frack.api.Worker;
import ezbake.security.client.EzbakeSecurityClient;
import ezbake.services.graph.thrift.EzGraphService;
import ezbake.services.graph.thrift.GraphName;
import ezbake.services.graph.thrift.types.DataType;
import ezbake.services.graph.thrift.types.Edge;
import ezbake.services.graph.thrift.types.EdgeLabel;
import ezbake.services.graph.thrift.types.ElementId;
import ezbake.services.graph.thrift.types.Graph;
import ezbake.services.graph.thrift.types.Property;
import ezbake.services.graph.thrift.types.PropertyKey;
import ezbake.services.graph.thrift.types.Vertex;
import ezbake.thrift.ThriftClientPool;

import ezbakehelpers.ezconfigurationhelpers.application.EzBakeApplicationConfigurationHelper;

/**
 * Adds relationships between Twitter users to EzGraph.
 */
public final class RelationshipWorker extends Worker<Tweet> {
    private static final long serialVersionUID = 3730754373957871205L;
    private static final Logger logger = LoggerFactory.getLogger(RelationshipWorker.class);

    /**
     * App and graph names for creating the schema and graph.
     */
    private static final String APP_NAME = "sampleApp";
    private static final String GRAPH_NAME = "sampleGraph";

    /**
     * Keys for properties that can be used in the graph.
     */
    private static final String SCREEN_NAME_KEY = "screenName";
    private static final String TWITTER_ID_KEY = "twitterId";
    private static final String TWEET_ID_KEY = "tweetId";
    private static final String EDGE_DESCRIPTOR_KEY = "edgeName";

    /**
     * A list of property keys to be used when writing a schema.
     */
    private static final List<PropertyKey> PROPERTY_KEYS = ImmutableList.of(
            new PropertyKey(SCREEN_NAME_KEY).setDataType(DataType.STRING),
            new PropertyKey(TWITTER_ID_KEY).setDataType(DataType.STRING),
            new PropertyKey(TWEET_ID_KEY).setDataType(DataType.STRING),
            new PropertyKey(EDGE_DESCRIPTOR_KEY).setDataType(DataType.STRING));

    /**
     * A list of EdgeLabels to be used when writing a schema.
     */
    private static final List<EdgeLabel> EDGE_LABELS = ImmutableList.of(
            new EdgeLabel(TweetRelationship.MENTIONED.getLabel()),
            new EdgeLabel(TweetRelationship.RETWEETED.getLabel()),
            new EdgeLabel(TweetRelationship.REPLIED_TO.getLabel()),
            new EdgeLabel(TweetRelationship.MENTIONED.getReverseLabel()),
            new EdgeLabel(TweetRelationship.RETWEETED.getReverseLabel()),
            new EdgeLabel(TweetRelationship.REPLIED_TO.getReverseLabel()));

    /**
     * Service name for graph service for accessing it's client through ThriftClientPool.
     */
    private static final String GRAPH_SERVICE_NAME = "graph-service";

    /**
     * Configuration property for setting the default visibility of the graph.
     */
    private static final String DEFAULT_GRAPH_VISIBILITY_CONF_KEY = "sampleapp.graph.visibility";

    /**
     * Pool from which to get Thrift clients.
     */
    private ThriftClientPool pool;

    /**
     * Security token used to communicate with EzGraphService and the client from which to get the token.
     */
    private EzSecurityToken token;
    private EzbakeSecurityClient securityClient;

    /**
     * GraphName object required by EzGraph.
     */
    private GraphName graphName;

    /**
     * Default constructor.
     */
    public RelationshipWorker() {
        super(Tweet.class);
    }

    /**
     * Returns a thrift graph based on a passed in Tweet. TwitterUsers are represented by vertices while edges represent
     * relationships which include 'replied to', 'retweeted' and 'mentioned' relationships. While Twitter considers all
     * these relationships to be "mentioned", 'retweeted' and 'repliedTo' have been moved into separate groups for
     * clarity"
     *
     * @param tweet The Tweet to be parsed for users and their relationships.
     * @param visibility The Visibility of the data to be written.
     * @return A thrift graph that can be written into the EzGraphService.
     */
    private static Graph parseTweetToGraph(Tweet tweet, Visibility visibility) {
        final Graph graph = new Graph();
        final TwitterUser author = tweet.getAuthor();

        writeUserToGraph(author, visibility, graph);

        final List<TwitterUser> mentionedUsers = tweet.getMentionedUsers();
        if (tweet.isSetRepliedTo()) {
            final TwitterUser repliedToUser = tweet.getRepliedTo().getAuthor();
            writeRelationships(TweetRelationship.REPLIED_TO, tweet, visibility, graph, repliedToUser);
            mentionedUsers.remove(repliedToUser);
        } else if (tweet.isSetRetweeted()) {
            final TwitterUser retweetedUser = tweet.getRetweeted().getAuthor();
            writeRelationships(TweetRelationship.RETWEETED, tweet, visibility, graph, retweetedUser);
            mentionedUsers.remove(retweetedUser);
        }
        writeRelationships(
                TweetRelationship.MENTIONED, tweet, visibility, graph,
                mentionedUsers.toArray(new TwitterUser[mentionedUsers.size()]));

        return graph;
    }

    /**
     * Writes a relationship between the Tweet author and each of a list of TwitterUsers.
     *
     * @param relationship The relationship to be written. E.g. "mentioned".
     * @param tweet The current Tweet being parsed. Contains information to be stored with the relationships.
     * @param visibility Visibility of the relationships to be written.
     * @param graph The graph to write the relationships to.
     * @param users The list of users to which a relationship will be written.
     */
    private static void writeRelationships(
            TweetRelationship relationship, Tweet tweet, Visibility visibility, Graph graph, TwitterUser... users) {
        final TwitterUser author = tweet.getAuthor();
        final String authorScreenName = author.getScreenName();

        for (final TwitterUser user : users) {
            final String screenName = user.getScreenName();
            writeUserToGraph(user, visibility, graph);
            writeRelationshipToGraph(relationship, authorScreenName, screenName, visibility, tweet.getId(), graph);
        }
    }

    /**
     * Writes a TwitterUser to the graph. Writing a TwitterUser essentially means creating a vertex with a selector
     * property (K,V) of "SCREEN_NAME, <Twitter User's screen name>"
     *
     * @param user The TwitterUser to be written.
     * @param visibility The Visibility this user will be written with.
     * @param graph The Graph to write this TwitterUser to.
     */
    private static void writeUserToGraph(TwitterUser user, Visibility visibility, Graph graph) {
        final String screenName = user.getScreenName();
        final ElementId vid = new ElementId();
        vid.setLocalId(screenName);

        final Map<String, List<Property>> props = getStandardUserProperties(user, visibility);
        logger.info("Writing user: {} with visiblity {}", screenName, visibility.getFormalVisibility());

        final Vertex vertex = new Vertex();
        vertex.setId(vid);
        vertex.setSelectorProperty(TWITTER_ID_KEY);
        vertex.setProperties(props);
        graph.addToVertices(vertex);
    }

    /**
     * Writes a relationship (edge) to an user from the author of the Tweet currently being parsed.
     *
     * @param relationship The type of relationship to be drawn, e.g. "mentioned".
     * @param authorScreenName Used to point to the 'out vertex' (authorName is the localId).
     * @param mentionedScreenName Used to point to the 'in vertex' (mentionedName is the localId).
     * @param visibility The Visibility this relationship will be written with.
     * @param tweetId The ID of the Tweet from which this relationship was parsed.
     * @param graph The Graph to write this relationship to.
     */
    private static void writeRelationshipToGraph(
            TweetRelationship relationship, String authorScreenName, String mentionedScreenName, Visibility visibility,
            String tweetId, Graph graph) {
        final Map<String, Property> props = Maps.newHashMap();
        props.put(TWEET_ID_KEY, GraphConverter.convertProperty(tweetId).setVisibility(visibility));
        final Property edgeDescriptorProp = GraphConverter.convertProperty(
                String.format("%s_%s_%s", authorScreenName, relationship.getLabel(), mentionedScreenName));

        props.put(EDGE_DESCRIPTOR_KEY, edgeDescriptorProp.setVisibility(visibility));
        final ElementId outVertexId = getUserElementId(authorScreenName);
        final ElementId inVertId = getUserElementId(mentionedScreenName);
        graph.addToEdges(buildEdge(props, visibility, relationship.getReverseLabel(), inVertId, outVertexId));
        graph.addToEdges(buildEdge(props, visibility, relationship.getLabel(), outVertexId, inVertId));

        logger.info(
                "Building new relationship: {} {} {}", authorScreenName, relationship.getLabel(), mentionedScreenName);
    }

    /**
     * Method to help building the vertices which represent TwitterUsers. Provides the properties for the vertex.
     *
     * @param twitterUser The TwitterUser from which vertex properties are derived.
     * @param visibility The Visibility with which the properties will be written.
     * @return The map of properties for the passed in TwitterUser.
     */
    private static Map<String, List<Property>> getStandardUserProperties(
            TwitterUser twitterUser, Visibility visibility) {
        final Map<String, List<Property>> props = Maps.newHashMap();

        final Property screenNameProp = new Property();
        screenNameProp.setValue(GraphConverter.convertObject(twitterUser.getScreenName()));
        screenNameProp.setVisibility(visibility);
        props.put(SCREEN_NAME_KEY, Lists.newArrayList(screenNameProp));

        final Property twitterIdProp = new Property();
        twitterIdProp.setValue(GraphConverter.convertObject(twitterUser.getId()));
        twitterIdProp.setVisibility(visibility);
        props.put(TWITTER_ID_KEY, Lists.newArrayList(twitterIdProp));

        return props;
    }

    /**
     * Convenience method for converting a TwitterUsers' screen name into an ElementId.
     *
     * @param screenName The screen name for the user.
     * @return The ElementId based on the screen name passed in.
     */
    private static ElementId getUserElementId(String screenName) {
        final ElementId vid = new ElementId();
        vid.setLocalId(screenName);
        return vid;
    }

    /**
     * Makes an edge describing a relationship.
     *
     * @param props Properties for the edge.
     * @param visibility The Visibility this relationship will be written with.
     * @param label The label for this edge.
     * @param outVertexId The vertex from which this relationship begins.
     * @param inVertexId The vertex at which this relationship terminates.
     * @return The fully build edge.
     */
    private static Edge buildEdge(
            Map<String, Property> props, Visibility visibility, String label, ElementId outVertexId,
            ElementId inVertexId) {
        final Edge edge = new Edge();
        edge.setProperties(props);
        edge.setVisibility(visibility);
        edge.setLabel(label);
        edge.setOutVertex(outVertexId);
        edge.setInVertex(inVertexId);
        return edge;
    }

    @Override
    public void initialize(Properties props) {
        super.initialize(props);
        EzGraphService.Client ezGraphClient = null;

        try {
            pool = new ThriftClientPool(props);

            securityClient = new EzbakeSecurityClient(props);
            final String securityId = new EzBakeApplicationConfigurationHelper(props).getSecurityID();
            token = securityClient.fetchAppToken(securityId);

            final Visibility visibility = new Visibility();
            visibility.setFormalVisibility(props.getProperty(DEFAULT_GRAPH_VISIBILITY_CONF_KEY, "U"));
            ezGraphClient = pool.getClient(GRAPH_SERVICE_NAME, EzGraphService.Client.class);
            graphName = new GraphName().setName(GRAPH_NAME);
            try {
                ezGraphClient.createSchema(APP_NAME, visibility, graphName, PROPERTY_KEYS, EDGE_LABELS, token);
            } catch (final Exception e) {
                logger.error("Could not write schema - schema has probably already been written");
            }
        } catch (final EzSecurityTokenException ex) {
            final String errMsg = "EzSecurity token error";
            logger.error(errMsg, ex);
            throw new RuntimeException(errMsg, ex);
        } catch (final TException ex) {
            final String errMsg = "Unable to get graph-service thrift client";
            logger.error("errMsg", ex);
            throw new RuntimeException(errMsg, ex);
        } finally {
            if (ezGraphClient != null) {
                pool.returnToPool(ezGraphClient);
            }
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
                logger.error("Could not close security client.", e);
            }
        }
    }

    @Override
    public void process(Visibility visibility, Tweet tweet) {
        EzGraphService.Client ezGraphClient = null;
        try {
            logger.info("Adding tweet with author: {}", tweet.getAuthor());
            ezGraphClient = pool.getClient(GRAPH_SERVICE_NAME, EzGraphService.Client.class);
            final Graph subGraph = parseTweetToGraph(tweet, visibility);
            ezGraphClient.writeGraph(APP_NAME, visibility, graphName, subGraph, token);
        } catch (final TException e) {
            logger.error("Thrift error", e);
            throw new RuntimeException(e);
        } finally {
            if (ezGraphClient != null) {
                pool.returnToPool(ezGraphClient);
            }
        }
    }

    /**
     * Keeps track of the various possible relationships defined in Tweets.
     */
    private enum TweetRelationship {
        MENTIONED("mentioned", "mentionedBy"),
        RETWEETED("retweeted", "retweetedBy"),
        REPLIED_TO("repliedTo", "repliedToBy");

        private final String label;
        private final String reverseLabel;

        /**
         * Constructor that assigns a value to label.
         *
         * @param label Short and simple description of this relationship.
         * @param reverseLabel Label used for making an edge in the opposite direction.
         */
        TweetRelationship(String label, String reverseLabel) {
            this.label = label;
            this.reverseLabel = reverseLabel;
        }

        /**
         * Returns this constants label value.
         *
         * @return The label value assigned to this constant.
         */
        public String getLabel() {
            return label;
        }

        /**
         * Returns the 'reverse' relationships' label used for graph traversal.
         *
         * @return The string used to label the 'reverse' relationship.
         */
        public String getReverseLabel() {
            return reverseLabel;
        }
    }
}
