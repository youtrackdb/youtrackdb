package com.jetbrains.youtrack.db.internal.core.db.record;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import javax.annotation.Nullable;

public interface LinkTrackedMultiValue<K> extends TrackedMultiValue<K, Identifiable> {

  @Nullable
  default Identifiable convertToRid(Identifiable e) {
    if (e == null) {
      return null;
    }
    var session = getSession();
    if (session == null) {
      throw new IllegalStateException(
          "Cannot add a record to a set that is not attached to a session");
    }
    e = session.refreshRid(e.getIdentity());
    return e;
  }
}
