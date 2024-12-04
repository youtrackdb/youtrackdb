package com.orientechnologies.orient.client.remote.message;

import com.orientechnologies.orient.core.id.YTRID;
import java.util.Map;

public class OSendTransactionStateResponse extends OBeginTransactionResponse {

  public OSendTransactionStateResponse() {
  }

  public OSendTransactionStateResponse(long txId,
      Map<YTRID, YTRID> updatedIds) {
    super(txId, updatedIds);
  }

}
