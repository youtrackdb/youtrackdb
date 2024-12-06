package com.orientechnologies.lucene.query;

import com.orientechnologies.lucene.collections.LuceneCompositeKey;
import java.util.Map;

/**
 *
 */
public class OLuceneKeyAndMetadata {

  public final LuceneCompositeKey key;
  public final Map<String, ?> metadata;

  public OLuceneKeyAndMetadata(final LuceneCompositeKey key, final Map<String, ?> metadata) {
    this.key = key;
    this.metadata = metadata;
  }
}
