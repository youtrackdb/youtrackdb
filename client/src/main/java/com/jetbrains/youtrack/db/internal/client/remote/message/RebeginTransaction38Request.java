package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import java.util.Map;

/**
 *
 */
public class RebeginTransaction38Request extends BeginTransaction38Request {

  public RebeginTransaction38Request(
      DatabaseSessionInternal session, long txId,
      boolean usingLong,
      Iterable<RecordOperation> operations,
      Map<String, FrontendTransactionIndexChanges> changes) {
    super(session, txId, true, usingLong, operations, changes);
  }

  public RebeginTransaction38Request() {
  }

  @Override
  public byte getCommand() {
    return ChannelBinaryProtocol.REQUEST_TX_REBEGIN;
  }

  @Override
  public String getDescription() {
    return "Re-begin transaction";
  }
}
