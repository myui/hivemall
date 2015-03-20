/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2015 Makoto YUI
 * Copyright (C) 2013-2015 National Institute of Advanced Industrial Science and Technology (AIST)
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
package hivemall.mix.store;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.concurrent.ThreadSafe;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

@ThreadSafe
public final class SessionStore {
    private static final int EXPECTED_MODEL_SIZE = 4194305; /* 2^22+1=4194304+1=4194305 */
    private static final Log logger = LogFactory.getLog(SessionStore.class);

    private final ConcurrentMap<String, SessionObject> sessions;

    public SessionStore() {
        this.sessions = new ConcurrentHashMap<String, SessionObject>();
    }

    @Nonnull
    private ConcurrentMap<String, SessionObject> getSessions() {
        return sessions;
    }

    @Nonnull
    public SessionObject get(@Nonnull String groupID) {
        SessionObject sessionObj = sessions.get(groupID);
        if(sessionObj == null) {
            ConcurrentMap<Object, PartialResult> map = new ConcurrentHashMap<Object, PartialResult>(EXPECTED_MODEL_SIZE);
            sessionObj = new SessionObject(map);
            SessionObject existing = sessions.putIfAbsent(groupID, sessionObj);
            if(existing != null) {
                sessionObj = existing;
            }
        }
        return sessionObj;
    }

    public void remove(@Nonnull String groupID) {
        SessionObject removedSession = sessions.remove(groupID);
        if(removedSession != null) {
            logger.info("Removed an idle session group: " + groupID + "\t"
                    + removedSession.getSessionInfo());
        }
    }

    @ThreadSafe
    public static final class IdleSessionSweeper implements Runnable {

        private final ConcurrentMap<String, SessionObject> sessions;
        private final long ttl;

        public IdleSessionSweeper(@Nonnull SessionStore sessionStore, @Nonnegative long ttlInMillis) {
            this.sessions = sessionStore.getSessions();
            this.ttl = ttlInMillis;
        }

        public void run() {
            final Set<Map.Entry<String, SessionObject>> entries = sessions.entrySet();
            final Iterator<Map.Entry<String, SessionObject>> itor = entries.iterator();
            while(itor.hasNext()) {
                Map.Entry<String, SessionObject> e = itor.next();
                SessionObject sessionObj = e.getValue();
                long lastAccessed = sessionObj.getLastAccessed();
                long elapsedTime = System.currentTimeMillis() - lastAccessed;
                if(elapsedTime > ttl) {
                    itor.remove();
                    if(logger.isInfoEnabled()) {
                        logger.info("Removed an idle session group: " + e.getKey() + "\t"
                                + sessionObj.getSessionInfo());
                    }
                }
            }

        }
    }

}
