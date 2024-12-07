package com.jetbrains.youtrack.db.internal.lucene.collections;

import com.jetbrains.youtrack.db.internal.common.util.RawPair;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import java.util.stream.Stream;

/**
 *
 */
public final class LuceneIndexTransformer {

  public static Stream<RawPair<Object, RID>> transformToStream(
      LuceneResultSet resultSet, Object key) {
    return resultSet.stream()
        .map((identifiable -> new RawPair<>(key, identifiable.getIdentity())));
  }
}
