package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.ORecordOperation;
import com.orientechnologies.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import java.util.Map;

/**
 *
 */
public class ORebeginTransaction38Request extends OBeginTransaction38Request {

  public ORebeginTransaction38Request(
      YTDatabaseSessionInternal session, long txId,
      boolean usingLong,
      Iterable<ORecordOperation> operations,
      Map<String, OTransactionIndexChanges> changes) {
    super(session, txId, true, usingLong, operations, changes);
  }

  public ORebeginTransaction38Request() {
  }

  @Override
  public byte getCommand() {
    return OChannelBinaryProtocol.REQUEST_TX_REBEGIN;
  }

  @Override
  public String getDescription() {
    return "Re-begin transaction";
  }
}
