package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.common.util.ORawPair;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurityShared;
import java.util.stream.Stream;

public class IndexStreamSecurityDecorator {

  public static Stream<ORawPair<Object, YTRID>> decorateStream(
      OIndex originalIndex, Stream<ORawPair<Object, YTRID>> stream) {
    YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return stream;
    }

    String indexClass = originalIndex.getDefinition().getClassName();
    if (indexClass == null) {
      return stream;
    }
    OSecurityInternal security = db.getSharedContext().getSecurity();
    if (security instanceof OSecurityShared
        && !((OSecurityShared) security).couldHaveActivePredicateSecurityRoles(db, indexClass)) {
      return stream;
    }

    return stream.filter(
        (pair) -> OIndexInternal.securityFilterOnRead(originalIndex, pair.second) != null);
  }

  public static Stream<YTRID> decorateRidStream(OIndex originalIndex, Stream<YTRID> stream) {
    YTDatabaseSessionInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (db == null) {
      return stream;
    }

    String indexClass = originalIndex.getDefinition().getClassName();
    if (indexClass == null) {
      return stream;
    }
    OSecurityInternal security = db.getSharedContext().getSecurity();
    if (security instanceof OSecurityShared
        && !((OSecurityShared) security).couldHaveActivePredicateSecurityRoles(db, indexClass)) {
      return stream;
    }

    return stream.filter((rid) -> OIndexInternal.securityFilterOnRead(originalIndex, rid) != null);
  }
}
