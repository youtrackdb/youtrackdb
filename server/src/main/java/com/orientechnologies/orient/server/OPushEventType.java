package com.orientechnologies.orient.server;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.orientechnologies.orient.client.remote.message.OBinaryPushRequest;
import com.orientechnologies.orient.server.network.protocol.binary.NetworkProtocolBinary;
import java.lang.ref.WeakReference;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class OPushEventType {

  private final ConcurrentMap<String, OBinaryPushRequest<?>> databases = new ConcurrentHashMap<>();
  protected final ConcurrentMap<String, Set<WeakReference<NetworkProtocolBinary>>> listeners =
      new ConcurrentHashMap<>();

  public synchronized void send(
      DatabaseSessionInternal session, String database, OBinaryPushRequest<?> request,
      PushManager pushManager) {
    OBinaryPushRequest<?> prev = databases.put(database, request);
    if (prev == null) {
      pushManager.genericNotify(session, listeners, database, this);
    }
  }

  public synchronized OBinaryPushRequest<?> getRequest(String database) {
    return databases.remove(database);
  }

  public synchronized void subscribe(String database, NetworkProtocolBinary protocol) {
    Set<WeakReference<NetworkProtocolBinary>> pushSockets = listeners.get(database);
    if (pushSockets == null) {
      pushSockets = new HashSet<>();
      listeners.put(database, pushSockets);
    }
    pushSockets.add(new WeakReference<>(protocol));
  }

  public synchronized void cleanListeners() {
    for (Set<WeakReference<NetworkProtocolBinary>> value : listeners.values()) {
      Iterator<WeakReference<NetworkProtocolBinary>> iter = value.iterator();
      while (iter.hasNext()) {
        if (iter.next().get() == null) {
          iter.remove();
        }
      }
    }
  }
}
