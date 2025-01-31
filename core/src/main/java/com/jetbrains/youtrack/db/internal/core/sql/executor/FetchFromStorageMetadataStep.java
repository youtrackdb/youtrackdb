package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.config.StorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.config.StorageEntryConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ProduceExecutionStream;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.StorageCluster;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Returns an Result containing metadata regarding the storage
 */
public class FetchFromStorageMetadataStep extends AbstractExecutionStep {

  public FetchFromStorageMetadataStep(CommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    if (prev != null) {
      prev.start(ctx).close(ctx);
    }

    return new ProduceExecutionStream(this::produce).limit(1);
  }

  private Result produce(CommandContext ctx) {
    var db = ctx.getDatabase();
    var result = new ResultInternal(db);

    var storage = db.getStorage();
    result.setProperty("clusters", toResult(db, storage.getClusterInstances()));
    result.setProperty("defaultClusterId", storage.getDefaultClusterId());
    result.setProperty("totalClusters", storage.getClusters());
    result.setProperty("configuration", toResult(db, storage.getConfiguration()));
    result.setProperty(
        "conflictStrategy",
        storage.getRecordConflictStrategy() == null
            ? null
            : storage.getRecordConflictStrategy().getName());
    result.setProperty("name", storage.getName());
    result.setProperty("size", storage.getSize(ctx.getDatabase()));
    result.setProperty("type", storage.getType());
    result.setProperty("version", storage.getVersion());
    result.setProperty("createdAtVersion", storage.getCreatedAtVersion());
    return result;
  }

  private static Object toResult(DatabaseSessionInternal db,
      StorageConfiguration configuration) {
    var result = new ResultInternal(db);
    result.setProperty("charset", configuration.getCharset());
    result.setProperty("clusterSelection", configuration.getClusterSelection());
    result.setProperty("conflictStrategy", configuration.getConflictStrategy());
    result.setProperty("dateFormat", configuration.getDateFormat());
    result.setProperty("dateTimeFormat", configuration.getDateTimeFormat());
    result.setProperty("localeCountry", configuration.getLocaleCountry());
    result.setProperty("localeLanguage", configuration.getLocaleLanguage());
    result.setProperty("recordSerializer", configuration.getRecordSerializer());
    result.setProperty("timezone", String.valueOf(configuration.getTimeZone()));
    result.setProperty("properties", toResult(db, configuration.getProperties()));
    return result;
  }

  private static List<Result> toResult(DatabaseSessionInternal db,
      List<StorageEntryConfiguration> properties) {
    List<Result> result = new ArrayList<>();
    if (properties != null) {
      for (var entry : properties) {
        var item = new ResultInternal(db);
        item.setProperty("name", entry.name);
        item.setProperty("value", entry.value);
        result.add(item);
      }
    }
    return result;
  }

  private List<Result> toResult(DatabaseSessionInternal db,
      Collection<? extends StorageCluster> clusterInstances) {
    List<Result> result = new ArrayList<>();
    if (clusterInstances != null) {
      for (var cluster : clusterInstances) {
        var item = new ResultInternal(db);
        item.setProperty("name", cluster.getName());
        item.setProperty("fileName", cluster.getFileName());
        item.setProperty("id", cluster.getId());
        item.setProperty("entries", cluster.getEntries());
        item.setProperty(
            "conflictStrategy",
            cluster.getRecordConflictStrategy() == null
                ? null
                : cluster.getRecordConflictStrategy().getName());
        item.setProperty("tombstonesCount", cluster.getTombstonesCount());
        result.add(item);
      }
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    var spaces = ExecutionStepInternal.getIndent(depth, indent);
    var result = spaces + "+ FETCH STORAGE METADATA";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
