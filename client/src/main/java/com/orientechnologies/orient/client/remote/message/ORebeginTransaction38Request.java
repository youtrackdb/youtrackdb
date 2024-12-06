package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.RecordOperation;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransactionIndexChanges;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.ChannelBinaryProtocol;
import java.util.Map;

/**
 *
 */
public class ORebeginTransaction38Request extends OBeginTransaction38Request {

  public ORebeginTransaction38Request(
      DatabaseSessionInternal session, long txId,
      boolean usingLong,
      Iterable<RecordOperation> operations,
      Map<String, FrontendTransactionIndexChanges> changes) {
    super(session, txId, true, usingLong, operations, changes);
  }

  public ORebeginTransaction38Request() {
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
