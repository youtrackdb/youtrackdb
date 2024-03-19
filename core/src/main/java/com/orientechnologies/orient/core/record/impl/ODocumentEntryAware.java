package com.orientechnologies.orient.core.record.impl;

/**
 *
 */
public interface ODocumentEntryAware {
  void setDocumentEntry(ODocumentEntry entry);

  void clearDocumentEntry();
}
