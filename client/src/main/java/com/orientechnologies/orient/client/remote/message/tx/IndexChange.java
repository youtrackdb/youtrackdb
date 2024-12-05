package com.orientechnologies.orient.client.remote.message.tx;

import com.jetbrains.youtrack.db.internal.core.tx.OTransactionIndexChanges;

/**
 *
 */
public class IndexChange {

  public IndexChange(String name, OTransactionIndexChanges keyChanges) {
    this.name = name;
    this.keyChanges = keyChanges;
  }

  private final String name;
  private final OTransactionIndexChanges keyChanges;

  public String getName() {
    return name;
  }

  public OTransactionIndexChanges getKeyChanges() {
    return keyChanges;
  }
}
