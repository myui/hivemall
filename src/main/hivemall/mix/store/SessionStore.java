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
    private static final Log logger = LogFactory.getLog(SessionStore.class);

    private static final int EXPECTED_MODEL_SIZE = 4194305; /* 2^22+1=4194304+1=4194305 */

    private final ConcurrentMap<String, SessionObject> sessions;

    public SessionStore() {
        this.sessions = new ConcurrentHashMap<String, SessionObject>();
    }

    @Nonnull
    private ConcurrentMap<String, SessionObject> getSessions() {
        return sessions;
    }

    @Nonnull
    public ConcurrentMap<Object, PartialResult> get(@Nonnull String groupID) {
        SessionObject sessionObj = sessions.get(groupID);
        if(sessionObj == null) {
            ConcurrentMap<Object, PartialResult> map = new ConcurrentHashMap<Object, PartialResult>(EXPECTED_MODEL_SIZE);
            sessionObj = new SessionObject(map);
            SessionObject existing = sessions.putIfAbsent(groupID, sessionObj);
            if(existing != null) {
                sessionObj = existing;
            }
        }
        ConcurrentMap<Object, PartialResult> map = sessionObj.get();
        sessionObj.touch();
        return map;
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
                    sessions.remove(key);
                    logger.info("Removed an idle session group: " + key);
                }
            }

        }
    }

}
