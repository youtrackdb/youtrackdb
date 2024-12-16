package com.jetbrains.youtrack.db.internal.server.distributed.operation;

import com.jetbrains.youtrack.db.internal.server.YouTrackDBServer;
import com.jetbrains.youtrack.db.internal.server.distributed.ODistributedServerManager;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public interface NodeOperation {

  NodeOperationResponse execute(YouTrackDBServer iServer, ODistributedServerManager iManager);

  void write(DataOutput out) throws IOException;

  void read(DataInput in) throws IOException;

  int getMessageId();
}
