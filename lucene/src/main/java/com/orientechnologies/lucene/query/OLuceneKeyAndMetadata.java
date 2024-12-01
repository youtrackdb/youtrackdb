package com.orientechnologies.lucene.query;

import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import java.util.Map;

/**
 *
 */
public class OLuceneKeyAndMetadata {

  public final OLuceneCompositeKey key;
  public final Map<String, ?> metadata;

  public OLuceneKeyAndMetadata(final OLuceneCompositeKey key, final Map<String, ?> metadata) {
    this.key = key;
    this.metadata = metadata;
  }
}
