package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.sql.executor.OResultSet;

/**
 *
 */
public class ORemoteQueryResult {

  private final OResultSet result;
  private final boolean transactionUpdated;
  private final boolean reloadMetadata;

  public ORemoteQueryResult(OResultSet result, boolean transactionUpdated, boolean reloadMetadata) {
    this.result = result;
    this.transactionUpdated = transactionUpdated;
    this.reloadMetadata = reloadMetadata;
  }

  public OResultSet getResult() {
    return result;
  }

  public boolean isTransactionUpdated() {
    return transactionUpdated;
  }

  public boolean isReloadMetadata() {
    return reloadMetadata;
  }
}
