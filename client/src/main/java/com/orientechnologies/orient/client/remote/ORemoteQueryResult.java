package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;

/**
 *
 */
public class ORemoteQueryResult {

  private final YTResultSet result;
  private final boolean transactionUpdated;
  private final boolean reloadMetadata;

  public ORemoteQueryResult(YTResultSet result, boolean transactionUpdated,
      boolean reloadMetadata) {
    this.result = result;
    this.transactionUpdated = transactionUpdated;
    this.reloadMetadata = reloadMetadata;
  }

  public YTResultSet getResult() {
    return result;
  }

  public boolean isTransactionUpdated() {
    return transactionUpdated;
  }

  public boolean isReloadMetadata() {
    return reloadMetadata;
  }
}
