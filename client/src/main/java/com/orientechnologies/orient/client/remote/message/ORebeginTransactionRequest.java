package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.tx.OTransactionIndexChanges;
import com.orientechnologies.orient.enterprise.channel.binary.OChannelBinaryProtocol;
import java.util.Map;

/**
 *
 */
public class ORebeginTransactionRequest extends OBeginTransactionRequest {

  public ORebeginTransactionRequest(
      YTDatabaseSessionInternal session, int txId,
      boolean usingLong,
      Iterable<ORecordOperation> operations,
      Map<String, OTransactionIndexChanges> changes) {
    super(session, txId, true, usingLong, operations, changes);
  }

  public ORebeginTransactionRequest() {
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
