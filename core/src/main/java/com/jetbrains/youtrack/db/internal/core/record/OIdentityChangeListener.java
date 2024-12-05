package com.jetbrains.youtrack.db.internal.core.record;

/**
 * Listener, which is called when record identity is changed. Identity is changed if new record is
 * saved or if transaction is committed and new record created inside of transaction.
 */
public interface OIdentityChangeListener {

  /**
   * Called before the change of the identity is made.
   *
   * @param record
   */
  void onBeforeIdentityChange(Record record);

  /**
   * called afer the change of the identity is made.
   *
   * @param record
   */
  void onAfterIdentityChange(Record record);
}
