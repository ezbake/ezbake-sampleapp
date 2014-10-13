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

import java.util.Set;

import javax.ws.rs.core.Application;

import com.google.common.collect.ImmutableSet;

import ezbake.app.sample.rest.resource.ImageResource;
import ezbake.app.sample.rest.resource.RelationshipResource;
import ezbake.app.sample.rest.resource.TweetResource;

/**
 * Web Application composed of SampleApp REST resources.
 */
public final class SampleWebApp extends Application {
    private final Set<Object> singletons =
            ImmutableSet.of(new RelationshipResource(), new TweetResource(), new ImageResource());

    private final Set<Class<?>> classes = ImmutableSet.<Class<?>>of(WebServiceExceptionHandler.class);

    @Override
    public Set<Class<?>> getClasses() {
        return classes;
    }

    @Override
    public Set<Object> getSingletons() {
        return singletons;
    }
}
