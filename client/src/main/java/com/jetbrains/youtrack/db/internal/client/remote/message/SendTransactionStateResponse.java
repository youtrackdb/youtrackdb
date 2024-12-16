package com.jetbrains.youtrack.db.internal.client.remote.message;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import java.util.Map;

public class SendTransactionStateResponse extends BeginTransactionResponse {

  public SendTransactionStateResponse() {
  }

  public SendTransactionStateResponse(long txId,
      Map<RecordId, RecordId> updatedIds) {
    super(txId, updatedIds);
  }

}
