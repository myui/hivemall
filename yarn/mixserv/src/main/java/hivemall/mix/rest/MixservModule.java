/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package hivemall.mix.rest;

import hivemall.mix.MixNodeManager;

import java.util.HashMap;

import javax.annotation.Nonnull;
import javax.ws.rs.ext.MessageBodyReader;
import javax.ws.rs.ext.MessageBodyWriter;

import org.codehaus.jackson.jaxrs.JacksonJsonProvider;
import org.eclipse.jetty.servlet.DefaultServlet;

import com.google.inject.Singleton;
import com.google.inject.servlet.ServletModule;
import com.sun.jersey.guice.spi.container.servlet.GuiceContainer;

public final class MixservModule extends ServletModule {

    @Nonnull
    private final MixNodeManager mixManager;

    public MixservModule(@Nonnull MixNodeManager mixManager) {
        this.mixManager = mixManager;
    }

    @Override
    protected void configureServlets() {
        bind(MixNodeManager.class).toInstance(mixManager);

        bind(DefaultServlet.class).in(Singleton.class);
        bind(HelloResource.class).in(Singleton.class);
        bind(MixservAPIv1Resource.class).in(Singleton.class);

        // bind Jackson converters for JAXB/JSON serialization
        bind(MessageBodyReader.class).to(JacksonJsonProvider.class);
        bind(MessageBodyWriter.class).to(JacksonJsonProvider.class);

        // Route all requests through GuiceContainer
        HashMap<String, String> options = new HashMap<String, String>();
        options.put("com.sun.jersey.api.json.POJOMappingFeature", "true");
        serve("/*").with(GuiceContainer.class, options);
    }

}
