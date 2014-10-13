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
import static ezbake.app.sample.rest.WebServiceUtils.isValidJsonObject;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ezbake.app.sample.rest.WebServiceException;
import ezbake.configuration.EzConfiguration;
import ezbake.configuration.EzConfigurationLoaderException;
import ezbake.data.elastic.thrift.MalformedQueryException;
import ezbake.data.image.frack.utilities.pojo.ImageSearchPojo;
import ezbake.data.image.frack.utilities.pojo.IndexedImagePojo;
import ezbake.data.image.frack.utilities.pojo.SearchResultsPojo;
import ezbake.services.indexing.image.thrift.ImageBinaryMissing;
import ezbake.services.indexing.image.thrift.ImageIndexerService;
import ezbake.services.indexing.image.thrift.ImageIndexerServiceConstants;
import ezbake.services.indexing.image.thrift.ImageSearch;
import ezbake.services.indexing.image.thrift.IndexedImage;
import ezbake.services.indexing.image.thrift.MaybeIndexedImage;
import ezbake.services.indexing.image.thrift.MaybeThumbnail;
import ezbake.services.indexing.image.thrift.SearchResults;
import ezbake.services.indexing.image.thrift.Thumbnail;
import ezbake.services.indexing.image.thrift.ThumbnailSize;
import ezbake.thrift.ThriftClientPool;

/**
 * REST endpoint to get and query images associated with Tweets.
 */
@Path("images")
public final class ImageResource {
    private static final Logger logger = LoggerFactory.getLogger(ImageResource.class);

    /**
     * Client pool used to create connections to the Image Indexer Thrift common service.
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
    public ImageResource() {
        try {
            final Properties props = new EzConfiguration().getProperties();
            pool = new ThriftClientPool(props);
        } catch (final EzConfigurationLoaderException e) {
            final String errMsg = "Could not read EzBake configuration";
            logger.error(errMsg, e);
            throw new WebApplicationException(
                    e, Response.status(INTERNAL_SERVER_ERROR).entity(errMsg).build());
        }
    }

    /**
     * Retrieves an image by its EzBake image ID.
     *
     * @param ezBakeImageId EzBake image ID of the image to retrieve
     * @return Response containing the binary and MIME type of the image
     */
    @GET
    @Path("binary/{ezBakeImageId : \\p{Alnum}{64}}")
    public Response getBinary(@PathParam("ezBakeImageId") String ezBakeImageId) {
        final IndexedImage image = retrieveImage(ezBakeImageId, true);
        return Response.ok(image.getImageData().getBlob(), image.getImageData().getMimeType()).build();
    }

    /**
     * Retrieves an image's metadata by its EzBake image ID.
     *
     * @param ezBakeImageId EzBake image ID of the image whose metadata to retrieve
     * @return JSON string of the metadata
     */
    @GET
    @Path("metadata/{ezBakeImageId : \\p{Alnum}{64}}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getMetadata(@PathParam("ezBakeImageId") String ezBakeImageId) {
        final IndexedImage image = retrieveImage(ezBakeImageId, false);

        try {
            return IndexedImagePojo.fromThrift(image).toJson(true, false);
        } catch (final TException e) {
            final String errMsg = String.format(
                    "Could not convert image metadata for image with EzBake image ID of '%s' to JSON", ezBakeImageId);

            logger.error(errMsg, e);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, errMsg);
        }
    }

    /**
     * Retrieves an image's thumbnail of the specified size.
     *
     * @param ezBakeImageId EzBake image ID of the image whose thumbnail to retrieve
     * @param size Size of the thumbnail (can be "small", "medium", or "large")
     * @return Response containing the binary and MIME type of the thumbnail
     */
    @GET
    @Path("thumbnail/{size}/{ezBakeImageId : \\p{Alnum}{64}}")
    public Response getThumbnail(@PathParam("ezBakeImageId") String ezBakeImageId, @PathParam("size") String size) {
        ImageIndexerService.Client client = null;
        try {
            client = getImageIndexerClient();

            final MaybeThumbnail maybeThumbnail = client.getThumbnail(
                    ezBakeImageId, ThumbnailSize.valueOf(size.toUpperCase()), getSecurityToken(httpRequest));

            if (!maybeThumbnail.isSetThumbnail()) {
                throw new WebServiceException(
                        NOT_FOUND, String.format(
                        "Thumbnail of size '%s' for EzBake image ID '%s' could not be found", size, ezBakeImageId));
            }

            final Thumbnail thumbnail = maybeThumbnail.getThumbnail();
            return Response.ok(thumbnail.getThumbnailBytes(), thumbnail.getMimeType()).build();
        } catch (final TException e) {
            final String errMsg = String.format(
                    "Thrift error when trying to retrieve thumbnail of size '%s' for image with EzBake image ID '%s'",
                    size, ezBakeImageId);

            logger.error(errMsg, e);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, errMsg);
        } finally {
            if (client != null) {
                pool.returnToPool(client);
            }
        }
    }

    /**
     * Search images by metadata or similarity via a JSON query.
     *
     * @param jsonQuery JSON image query
     * @return JSON string of the search results
     */
    @POST
    @Path("search")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String searchImages(String jsonQuery) {
        if (!isValidJsonObject(jsonQuery)) {
            final String errMsg = "Image search query must be a valid JSON object";
            logger.error(errMsg);
            throw new WebServiceException(BAD_REQUEST, errMsg);
        }

        ImageSearch search = null;
        try {
            search = ImageSearchPojo.fromJson(jsonQuery).toThrift();
        } catch (final TException e) {
            final String errMsg = "JSON object is not a valid image search query";
            logger.error(errMsg, e);
            throw new WebServiceException(BAD_REQUEST, errMsg);
        }

        ImageIndexerService.Client client = null;
        try {
            client = getImageIndexerClient();
            final SearchResults results = client.searchImages(search, getSecurityToken(httpRequest));
            return SearchResultsPojo.fromThrift(results).toJson();
        } catch (final MalformedQueryException e) {
            final String errMsg = "Malformed image search query";
            logger.error(errMsg, e);
            throw new WebServiceException(BAD_REQUEST, errMsg + ": " + e.getMessage());
        } catch (final TException e) {
            final String errMsg = "Thrift error when trying to search images with query: " + jsonQuery;
            logger.error(errMsg, e);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, errMsg);
        } finally {
            if (client != null) {
                pool.returnToPool(client);
            }
        }
    }

    /**
     * Retrieves an image from the image indexing service.
     *
     * @param ezBakeImageId EzBake image ID of the image to retrieve
     * @param withBinary true to retrieve binary along with metadata, false for just metadata
     * @return Retrieved image
     */
    private IndexedImage retrieveImage(String ezBakeImageId, boolean withBinary) {
        ImageIndexerService.Client client = null;
        try {
            client = getImageIndexerClient();
            MaybeIndexedImage maybeImage = null;
            if (withBinary) {
                maybeImage = client.getImageWithBinary(ezBakeImageId, getSecurityToken(httpRequest));
            } else {
                maybeImage = client.getImage(ezBakeImageId, getSecurityToken(httpRequest));
            }

            if (!maybeImage.isSetIndexedImage()) {
                throw new WebServiceException(
                        NOT_FOUND, String.format("Image with EzBake image ID '%s' not found", ezBakeImageId));
            }

            return maybeImage.getIndexedImage();
        } catch (final ImageBinaryMissing e) {
            final String errMsg = String.format(
                    "Image binary is missing for image with EzBake image ID '%s'", ezBakeImageId);

            logger.error(errMsg, e);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, errMsg);
        } catch (final TException e) {
            final String errMsg = String.format(
                    "Thrift error when trying to retrieve binary for image with EzBake image ID '%s'", ezBakeImageId);

            logger.error(errMsg, e);
            throw new WebServiceException(INTERNAL_SERVER_ERROR, errMsg);
        } finally {
            if (client != null) {
                pool.returnToPool(client);
            }
        }
    }

    /**
     * Gets an image indexer service client from the Thrift client pool.
     *
     * @return An image indexer service client
     * @throws TException if the client could not be retrieved from the pool
     */
    private ImageIndexerService.Client getImageIndexerClient() throws TException {
        return pool.getClient(ImageIndexerServiceConstants.SERVICE_NAME, ImageIndexerService.Client.class);
    }
}
