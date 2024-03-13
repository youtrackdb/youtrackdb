package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.id.ChangeableIdentity;
import com.orientechnologies.orient.core.id.IdentityChangeListener;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class RecordListenersManager implements IdentityChangeListener {
  private final HashMap<ORID, HashSet<ListenerWeakRef>> recordListeners = new HashMap<>();
  private final IdentityHashMap<ORID, HashSet<ListenerWeakRef>> identityChangeMap =
      new IdentityHashMap<>();
  private final ReferenceQueue<RecordListener> refQueue = new ReferenceQueue<>();

  private void evictStaleEntries() {
    ListenerWeakRef sv;
    while ((sv = (ListenerWeakRef) refQueue.poll()) != null) {
      final ORID key = sv.rid;
      var listeners = recordListeners.get(key);
      if (listeners != null) {
        listeners.remove(sv);
        if (listeners.isEmpty()) {
          recordListeners.remove(key);
          if (key instanceof ChangeableIdentity changeableIdentity) {
            changeableIdentity.removeIdentityChangeListener(this);
          }
        }
      }
    }
  }

  public void addRecordListener(ORecord record, RecordListener listener) {
    evictStaleEntries();

    var rid = record.getIdentity();
    var listeners = recordListeners.computeIfAbsent(rid, k -> new HashSet<>());
    listeners.add(new ListenerWeakRef(rid, listener, refQueue));

    if (rid instanceof ChangeableIdentity changeableIdentity &&
        changeableIdentity.canChangeIdentity()) {
      changeableIdentity.addIdentityChangeListener(this);
    }
  }

  public void removeRecordListener(ORecord record, RecordListener listener) {
    evictStaleEntries();

    var rid = record.getIdentity();
    var listeners = recordListeners.get(rid);
    if (listeners != null) {
      listeners.remove(new ListenerWeakRef(rid, listener, refQueue));

      if (listeners.isEmpty()) {
        recordListeners.remove(rid);
        if (rid instanceof ChangeableIdentity changeableIdentity) {
          changeableIdentity.removeIdentityChangeListener(this);
        }
      }
    }
  }

  public void clearRecordListeners(ORecord record) {
    evictStaleEntries();

    var rid = record.getIdentity();
    recordListeners.remove(rid);
    if (rid instanceof ChangeableIdentity changeableIdentity) {
      changeableIdentity.removeIdentityChangeListener(this);
    }
  }

  public void clear() {
    recordListeners.clear();
    identityChangeMap.clear();
  }

  public void triggerRecordListeners(ORecord record) {
    evictStaleEntries();

    var rid = record.getIdentity();
    var listeners = recordListeners.get(rid);

    if (listeners != null) {
      for (var listener : listeners) {
        var referent = listener.get();
        if (referent != null) {
          referent.onRecordChange(record);
        }
      }
    }
  }

  @Override
  public void onBeforeIdentityChange(Object source) {
    var rid = (ORID) source;
    var listeners = recordListeners.remove(rid);
    if (listeners != null) {
      identityChangeMap.put(rid, listeners);
    }
  }

  @Override
  public void onAfterIdentityChange(Object source) {
    var rid = (ORID) source;
    var listeners = identityChangeMap.remove(rid);
    if (listeners != null) {
      recordListeners.put(rid, listeners);
    }
  }

  private static final class ListenerWeakRef extends WeakReference<RecordListener> {
    private final ORID rid;
    private final long listenerId;

    public ListenerWeakRef(
        ORID rid, RecordListener referent, ReferenceQueue<? super RecordListener> q) {
      super(referent, q);
      this.rid = rid;
      this.listenerId = referent.id;
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      ListenerWeakRef that = (ListenerWeakRef) o;
      return rid.equals(that.rid) && listenerId == that.listenerId;
    }

    public int hashCode() {
      return 31 * Long.hashCode(listenerId) + 103 * rid.hashCode();
    }

    public String toString() {
      return ListenerWeakRef.class.getSimpleName()
          + "{"
          + "rid="
          + rid
          + ", listenerId="
          + listenerId
          + '}';
    }
  }

  public static abstract class RecordListener {
    private static final AtomicLong nextId = new AtomicLong(0);
    private long id = nextId.getAndIncrement();

    public abstract void onRecordChange(ORecord record);

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      RecordListener that = (RecordListener) o;
      return id == that.id;
    }

    public int hashCode() {
      return Long.hashCode(id);
    }

    public String toString() {
      return RecordListener.class.getSimpleName() + "{" + "id=" + id + '}';
    }
  }
}
