package com.orientechnologies.lucene.query;

import com.orientechnologies.lucene.collections.OLuceneCompositeKey;
import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 *
 */
public class OLuceneKeyAndMetadata {

  public final OLuceneCompositeKey key;
  public final ODocument metadata;

  public OLuceneKeyAndMetadata(final OLuceneCompositeKey key, final ODocument metadata) {
    this.key = key;
    this.metadata = metadata;
  }
}
