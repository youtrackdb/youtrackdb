package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.id.RID;
import java.util.Map;

public class SendTransactionStateResponse extends BeginTransactionResponse {

  public SendTransactionStateResponse() {
  }

  public SendTransactionStateResponse(long txId,
      Map<RID, RID> updatedIds) {
    super(txId, updatedIds);
  }

}
