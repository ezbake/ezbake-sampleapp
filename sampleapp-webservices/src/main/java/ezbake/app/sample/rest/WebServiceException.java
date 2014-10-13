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

import javax.ws.rs.core.Response;

/**
 * Exception for WebService errors.
 */
public final class WebServiceException extends RuntimeException {
    private static final long serialVersionUID = -8669735690054437389L;

    /**
     * HTTP status of the problem.
     */
    private final Response.Status status;

    /**
     * Constructor.
     *
     * @param status HTTP status of the problem
     * @param message Error explanation
     */
    public WebServiceException(Response.Status status, String message) {
        super(message);
        this.status = status;
    }

    /**
     * Getter for HTTP status of the problem.
     *
     * @return HTTP status of the problem
     */
    public Response.Status getStatus() {
        return status;
    }
}
