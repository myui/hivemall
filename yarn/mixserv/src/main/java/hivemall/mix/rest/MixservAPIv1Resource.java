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
import hivemall.mix.api.NodeId;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

import com.google.common.base.Preconditions;
import com.google.inject.Inject;

@Path("/api/v1/mixserv")
public class MixservAPIv1Resource {

    @Nonnull
    private final MixNodeManager mixManager;

    @Inject
    public MixservAPIv1Resource(@CheckForNull MixNodeManager mixManager) {
        this.mixManager = Preconditions.checkNotNull(mixManager);
    }

    @POST
    @Consumes({MediaType.APPLICATION_JSON})
    @Path("/join")
    public void join(NodeId nodeId) {
        
    }

}
