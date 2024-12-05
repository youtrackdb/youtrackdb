package com.orientechnologies.orient.client.remote.message;

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionStep;
import com.jetbrains.youtrack.db.internal.core.sql.executor.InfoExecutionStep;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.OInfoExecutionPlan;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataInput;
import com.jetbrains.youtrack.db.internal.enterprise.channel.binary.OChannelDataOutput;
import com.orientechnologies.orient.client.remote.OBinaryResponse;
import com.orientechnologies.orient.client.remote.OStorageRemoteSession;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 *
 */
public class OQueryResponse implements OBinaryResponse {

  public static final byte RECORD_TYPE_BLOB = 0;
  public static final byte RECORD_TYPE_VERTEX = 1;
  public static final byte RECORD_TYPE_EDGE = 2;
  public static final byte RECORD_TYPE_ELEMENT = 3;
  public static final byte RECORD_TYPE_PROJECTION = 4;

  private String queryId;
  private boolean txChanges;
  private List<YTResult> result;
  private Optional<OExecutionPlan> executionPlan;
  private boolean hasNextPage;
  private Map<String, Long> queryStats;
  private boolean reloadMetadata;

  public OQueryResponse(
      String queryId,
      boolean txChanges,
      List<YTResult> result,
      Optional<OExecutionPlan> executionPlan,
      boolean hasNextPage,
      Map<String, Long> queryStats,
      boolean reloadMetadata) {
    this.queryId = queryId;
    this.txChanges = txChanges;
    this.result = result;
    this.executionPlan = executionPlan;
    this.hasNextPage = hasNextPage;
    this.queryStats = queryStats;
    this.reloadMetadata = reloadMetadata;
  }

  public OQueryResponse() {
  }

  @Override
  public void write(YTDatabaseSessionInternal session, OChannelDataOutput channel,
      int protocolVersion, ORecordSerializer serializer)
      throws IOException {
    channel.writeString(queryId);
    channel.writeBoolean(txChanges);
    writeExecutionPlan(session, executionPlan, channel, serializer);
    // THIS IS A PREFETCHED COLLECTION NOT YET HERE
    channel.writeInt(0);
    channel.writeInt(result.size());
    for (YTResult res : result) {
      OMessageHelper.writeResult(session, res, channel, serializer);
    }
    channel.writeBoolean(hasNextPage);
    writeQueryStats(queryStats, channel);
    channel.writeBoolean(reloadMetadata);
  }

  @Override
  public void read(YTDatabaseSessionInternal db, OChannelDataInput network,
      OStorageRemoteSession session) throws IOException {
    queryId = network.readString();
    txChanges = network.readBoolean();
    executionPlan = readExecutionPlan(db, network);
    // THIS IS A PREFETCHED COLLECTION NOT YET HERE
    int prefetched = network.readInt();
    int size = network.readInt();
    this.result = new ArrayList<>(size);
    while (size-- > 0) {
      result.add(OMessageHelper.readResult(db, network));
    }
    this.hasNextPage = network.readBoolean();
    this.queryStats = readQueryStats(network);
    reloadMetadata = network.readBoolean();
  }

  private void writeQueryStats(Map<String, Long> queryStats, OChannelDataOutput channel)
      throws IOException {
    if (queryStats == null) {
      channel.writeInt(0);
      return;
    }
    channel.writeInt(queryStats.size());
    for (Map.Entry<String, Long> entry : queryStats.entrySet()) {
      channel.writeString(entry.getKey());
      channel.writeLong(entry.getValue());
    }
  }

  private Map<String, Long> readQueryStats(OChannelDataInput channel) throws IOException {
    Map<String, Long> result = new HashMap<>();
    int size = channel.readInt();
    for (int i = 0; i < size; i++) {
      String key = channel.readString();
      Long val = channel.readLong();
      result.put(key, val);
    }
    return result;
  }

  private static void writeExecutionPlan(
      YTDatabaseSessionInternal session, Optional<OExecutionPlan> executionPlan,
      OChannelDataOutput channel,
      ORecordSerializer recordSerializer)
      throws IOException {
    if (executionPlan.isPresent()
        && GlobalConfiguration.QUERY_REMOTE_SEND_EXECUTION_PLAN.getValueAsBoolean()) {
      channel.writeBoolean(true);
      OMessageHelper.writeResult(session, executionPlan.get().toResult(session), channel,
          recordSerializer);
    } else {
      channel.writeBoolean(false);
    }
  }

  private Optional<OExecutionPlan> readExecutionPlan(YTDatabaseSessionInternal db,
      OChannelDataInput network) throws IOException {
    boolean present = network.readBoolean();
    if (!present) {
      return Optional.empty();
    }
    OInfoExecutionPlan result = new OInfoExecutionPlan();
    YTResult read = OMessageHelper.readResult(db, network);
    result.setCost(((Number) read.getProperty("cost")).intValue());
    result.setType(read.getProperty("type"));
    result.setJavaType(read.getProperty("javaType"));
    result.setPrettyPrint(read.getProperty("prettyPrint"));
    result.setStmText(read.getProperty("stmText"));
    List<YTResult> subSteps = read.getProperty("steps");
    if (subSteps != null) {
      subSteps.forEach(x -> result.getSteps().add(toInfoStep(x)));
    }
    return Optional.of(result);
  }

  public String getQueryId() {
    return queryId;
  }

  public List<YTResult> getResult() {
    return result;
  }

  public Optional<OExecutionPlan> getExecutionPlan() {
    return executionPlan;
  }

  public boolean isHasNextPage() {
    return hasNextPage;
  }

  public Map<String, Long> getQueryStats() {
    return queryStats;
  }

  private ExecutionStep toInfoStep(YTResult x) {
    InfoExecutionStep result = new InfoExecutionStep();
    result.setName(x.getProperty("name"));
    result.setType(x.getProperty("type"));
    result.setTargetNode(x.getProperty("targetNode"));
    result.setJavaType(x.getProperty("javaType"));
    result.setCost(x.getProperty("cost") == null ? -1 : x.getProperty("cost"));
    List<YTResult> ssteps = x.getProperty("subSteps");
    if (ssteps != null) {
      ssteps.stream().forEach(sstep -> result.getSubSteps().add(toInfoStep(sstep)));
    }
    result.setDescription(x.getProperty("description"));
    return result;
  }

  public boolean isTxChanges() {
    return txChanges;
  }

  public boolean isReloadMetadata() {
    return reloadMetadata;
  }
}
