package com.orientechnologies.orient.client.remote.message.tx;

import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;

/**
 *
 */
public class IndexChange {

  public IndexChange(String name, FrontendTransactionIndexChanges keyChanges) {
    this.name = name;
    this.keyChanges = keyChanges;
  }

  private final String name;
  private final FrontendTransactionIndexChanges keyChanges;

  public String getName() {
    return name;
  }

  public FrontendTransactionIndexChanges getKeyChanges() {
    return keyChanges;
  }
}
