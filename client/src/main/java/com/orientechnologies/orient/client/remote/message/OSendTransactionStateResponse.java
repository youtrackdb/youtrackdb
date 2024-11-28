package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.id.ORID;
import java.util.Map;

public class OSendTransactionStateResponse extends OBeginTransactionResponse {

  public OSendTransactionStateResponse() {
  }

  public OSendTransactionStateResponse(long txId,
      Map<ORID, ORID> updatedIds) {
    super(txId, updatedIds);
  }

}
