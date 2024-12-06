package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.id.RID;
import java.util.Map;

public class OSendTransactionStateResponse extends OBeginTransactionResponse {

  public OSendTransactionStateResponse() {
  }

  public OSendTransactionStateResponse(long txId,
      Map<RID, RID> updatedIds) {
    super(txId, updatedIds);
  }

}
