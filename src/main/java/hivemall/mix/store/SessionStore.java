/*
 * Hivemall: Hive scalable Machine Learning Library
 *
 * Copyright (C) 2013-2014
 *   National Institute of Advanced Industrial Science and Technology (AIST)
 *   Registration Number: H25PRO-1520
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA
 */
package hivemall.mix.store;

import java.util.Map;
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
            for(Map.Entry<String, SessionObject> e : sessions.entrySet()) {
                SessionObject sessionObj = e.getValue();
                long lastAccessed = sessionObj.getLastAccessed();
                long elapsedTime = System.currentTimeMillis() - lastAccessed;
                if(elapsedTime > ttl) {
                    String key = e.getKey();
                    assert (key != null);
                    SessionObject removedSession = sessions.remove(key);
                    if(removedSession != null) {
                        logger.info("Removed an idle session group: " + key + "\t"
                                + removedSession.getSessionInfo());
                    }
                }
            }

        }
    }

}
