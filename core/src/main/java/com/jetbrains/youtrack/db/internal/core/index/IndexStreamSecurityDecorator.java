package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityShared;
import java.util.stream.Stream;

public class IndexStreamSecurityDecorator {

  public static Stream<RawPair<Object, RID>> decorateStream(
      Index originalIndex, Stream<RawPair<Object, RID>> stream) {
    var db = DatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return stream;
    }

    var indexClass = originalIndex.getDefinition().getClassName();
    if (indexClass == null) {
      return stream;
    }
    var security = db.getSharedContext().getSecurity();
    if (security instanceof SecurityShared
        && !((SecurityShared) security).couldHaveActivePredicateSecurityRoles(db, indexClass)) {
      return stream;
    }

    return stream.filter(
        (pair) -> IndexInternal.securityFilterOnRead(db, originalIndex, pair.second) != null);
  }

  public static Stream<RID> decorateRidStream(Index originalIndex, Stream<RID> stream) {
    var db = DatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return stream;
    }

    var indexClass = originalIndex.getDefinition().getClassName();
    if (indexClass == null) {
      return stream;
    }
    var security = db.getSharedContext().getSecurity();
    if (security instanceof SecurityShared
        && !((SecurityShared) security).couldHaveActivePredicateSecurityRoles(db, indexClass)) {
      return stream;
    }

    return stream.filter(
        (rid) -> IndexInternal.securityFilterOnRead(db, originalIndex, rid) != null);
  }
}
