package com.jetbrains.youtrack.db.internal.core.cache;

import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableIdentity;
import com.jetbrains.youtrack.db.internal.core.id.IdentityChangeListener;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.AbstractMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.BiConsumer;
import javax.annotation.Nonnull;

/**
 * Cache implementation that uses Weak References.
 */
public final class ORIDsWeakValuesHashMap<V> extends AbstractMap<YTRID, V>
    implements IdentityChangeListener {

  private final ReferenceQueue<V> refQueue = new ReferenceQueue<>();

  private final HashMap<YTRID, WeakRefValue<V>> hashMap = new HashMap<>();

  private boolean stopModification = false;

  /**
   * Map that is used to keep records between before identity change and after identity change
   * events.
   */
  private final IdentityHashMap<YTRID, V> identityChangeMap = new IdentityHashMap<>();

  public V get(YTRID key) {
    evictStaleEntries();
    V result = null;
    final WeakRefValue<V> soft_ref = hashMap.get(key);

    if (soft_ref != null) {
      result = soft_ref.get();
      if (result == null) {
        if (stopModification) {
          throw new IllegalStateException("Modification is not allowed");
        }

        hashMap.remove(key);
        if (key instanceof ChangeableIdentity changeableIdentity) {
          changeableIdentity.removeIdentityChangeListener(this);
        }
      }
    }
    return result;
  }

  private void evictStaleEntries() {
    if (stopModification) {
      throw new IllegalStateException("Modification is not allowed");
    }

    int evicted = 0;

    WeakRefValue<V> sv;
    //noinspection unchecked
    while ((sv = (WeakRefValue<V>) refQueue.poll()) != null) {
      final YTRID key = sv.key;

      if (key instanceof ChangeableIdentity changeableIdentity) {
        changeableIdentity.removeIdentityChangeListener(this);
      }

      hashMap.remove(key);
      evicted++;
    }

    if (evicted > 0) {
      OLogManager.instance().debug(this, "Evicted %d items", evicted);
    }
  }

  public V put(final YTRID key, final V value) {
    if (stopModification) {
      throw new IllegalStateException("Modification is not allowed");
    }

    evictStaleEntries();

    final WeakRefValue<V> soft_ref = new WeakRefValue<>(key, value, refQueue);

    if (key instanceof ChangeableIdentity changeableIdentity) {
      changeableIdentity.addIdentityChangeListener(this);
    }

    final WeakRefValue<V> result = hashMap.put(key, soft_ref);
    if (result == null) {
      return null;
    }

    return result.get();
  }

  public V remove(YTRID key) {
    if (stopModification) {
      throw new IllegalStateException("Modification is not allowed");
    }

    evictStaleEntries();

    final WeakRefValue<V> result = hashMap.remove(key);
    if (key instanceof ChangeableIdentity changeableIdentity) {
      changeableIdentity.removeIdentityChangeListener(this);
    }

    if (result == null) {
      return null;
    }

    return result.get();
  }

  public void clear() {
    if (stopModification) {
      throw new IllegalStateException("Modification is not allowed");
    }

    hashMap.clear();
  }

  public int size() {
    evictStaleEntries();

    return hashMap.size();
  }

  public @Nonnull Set<Entry<YTRID, V>> entrySet() {
    evictStaleEntries();
    Set<Entry<YTRID, V>> result = new HashSet<>();
    for (final Entry<YTRID, WeakRefValue<V>> entry : hashMap.entrySet()) {
      final V value = entry.getValue().get();
      if (value != null) {
        result.add(
            new Entry<>() {
              public YTRID getKey() {
                return entry.getKey();
              }

              public V getValue() {
                return value;
              }

              public V setValue(V v) {
                throw new UnsupportedOperationException();
              }
            });
      }
    }
    return result;
  }

  @Override
  public void forEach(BiConsumer<? super YTRID, ? super V> action) {
    evictStaleEntries();

    stopModification = true;
    try {
      for (final Entry<YTRID, WeakRefValue<V>> entry : hashMap.entrySet()) {
        final V value = entry.getValue().get();
        if (value != null) {
          action.accept(entry.getKey(), value);
        }
      }
    } finally {
      stopModification = false;
    }
  }

  @Override
  public void onBeforeIdentityChange(Object source) {
    var rid = (YTRID) source;
    if (stopModification) {
      throw new IllegalStateException("Modification is not allowed");
    }

    var record = hashMap.remove(rid);

    if (record != null) {
      var recordValue = record.get();

      if (recordValue != null) {
        identityChangeMap.put(rid, recordValue);
      }
    }
  }

  @Override
  public void onAfterIdentityChange(Object source) {
    if (stopModification) {
      throw new IllegalStateException("Modification is not allowed");
    }

    var rid = (YTRID) source;
    var record = identityChangeMap.remove(rid);

    if (record != null) {
      hashMap.put(rid, new WeakRefValue<>(rid, record, refQueue));
    }
  }

  private static final class WeakRefValue<V> extends WeakReference<V> {

    private final @Nonnull YTRID key;

    public WeakRefValue(
        @Nonnull final YTRID key, @Nonnull final V value, final ReferenceQueue<V> queue) {
      super(value, queue);
      this.key = key;
    }

    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      @SuppressWarnings("unchecked")
      WeakRefValue<V> that = (WeakRefValue<V>) o;
      return key.equals(that.key);
    }

    public int hashCode() {
      return key.hashCode();
    }

    public String toString() {
      return ORIDsWeakValuesHashMap.class.getSimpleName() + " {" + "key=" + key + '}';
    }
  }
}
