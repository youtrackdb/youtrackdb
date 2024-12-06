package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityShared;
import java.util.stream.Stream;

public class IndexStreamSecurityDecorator {

  public static Stream<RawPair<Object, RID>> decorateStream(
      Index originalIndex, Stream<RawPair<Object, RID>> stream) {
    DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return stream;
    }

    String indexClass = originalIndex.getDefinition().getClassName();
    if (indexClass == null) {
      return stream;
    }
    SecurityInternal security = db.getSharedContext().getSecurity();
    if (security instanceof SecurityShared
        && !((SecurityShared) security).couldHaveActivePredicateSecurityRoles(db, indexClass)) {
      return stream;
    }

    return stream.filter(
        (pair) -> IndexInternal.securityFilterOnRead(originalIndex, pair.second) != null);
  }

  public static Stream<RID> decorateRidStream(Index originalIndex, Stream<RID> stream) {
    DatabaseSessionInternal db = DatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return stream;
    }

    String indexClass = originalIndex.getDefinition().getClassName();
    if (indexClass == null) {
      return stream;
    }
    SecurityInternal security = db.getSharedContext().getSecurity();
    if (security instanceof SecurityShared
        && !((SecurityShared) security).couldHaveActivePredicateSecurityRoles(db, indexClass)) {
      return stream;
    }

    return stream.filter((rid) -> IndexInternal.securityFilterOnRead(originalIndex, rid) != null);
  }
}
