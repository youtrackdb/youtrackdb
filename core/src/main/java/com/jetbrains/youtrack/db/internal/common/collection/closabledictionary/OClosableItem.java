package com.jetbrains.youtrack.db.internal.common.collection.closabledictionary;

/**
 * Item is going to be stored inside of {@link OClosableLinkedContainer}. This interface presents
 * item that may be in two states open and closed.
 */
public interface OClosableItem {

  boolean isOpen();

  void close();

  void open();
}
