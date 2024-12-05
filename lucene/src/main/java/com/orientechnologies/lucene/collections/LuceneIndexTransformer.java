package com.orientechnologies.lucene.collections;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.core.id.YTRID;
import java.util.stream.Stream;

/**
 *
 */
public final class LuceneIndexTransformer {

  public static Stream<ORawPair<Object, YTRID>> transformToStream(
      OLuceneResultSet resultSet, Object key) {
    return resultSet.stream()
        .map((identifiable -> new ORawPair<>(key, identifiable.getIdentity())));
  }
}
