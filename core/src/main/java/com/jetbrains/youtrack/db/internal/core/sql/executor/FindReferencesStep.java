package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.concur.TimeoutException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.resultset.ExecutionStream;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLCluster;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLIdentifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 *
 */
public class FindReferencesStep extends AbstractExecutionStep {

  private final List<SQLIdentifier> classes;
  private final List<SQLCluster> clusters;

  public FindReferencesStep(
      List<SQLIdentifier> classes,
      List<SQLCluster> clusters,
      CommandContext ctx,
      boolean profilingEnabled) {
    super(ctx, profilingEnabled);
    this.classes = classes;
    this.clusters = clusters;
  }

  @Override
  public ExecutionStream internalStart(CommandContext ctx) throws TimeoutException {
    var db = ctx.getDatabase();
    Set<RID> rids = fetchRidsToFind(ctx);
    List<RecordIteratorCluster<DBRecord>> clustersIterators = initClusterIterators(ctx);
    Stream<Result> stream =
        clustersIterators.stream()
            .flatMap(
                (iterator) -> {
                  return StreamSupport.stream(
                          Spliterators.spliteratorUnknownSize(iterator, 0), false)
                      .flatMap((record) -> findMatching(db, rids, record));
                });
    return ExecutionStream.resultIterator(stream.iterator());
  }

  private static Stream<? extends Result> findMatching(DatabaseSessionInternal db,
      Set<RID> rids,
      DBRecord record) {
    ResultInternal rec = new ResultInternal(db, record);
    List<Result> results = new ArrayList<>();
    for (RID rid : rids) {
      List<String> resultForRecord = checkObject(Collections.singleton(rid), rec, record, "");
      if (!resultForRecord.isEmpty()) {
        ResultInternal nextResult = new ResultInternal(db);
        nextResult.setProperty("rid", rid);
        nextResult.setProperty("referredBy", rec);
        nextResult.setProperty("fields", resultForRecord);
        results.add(nextResult);
      }
    }
    return results.stream();
  }

  private List<RecordIteratorCluster<DBRecord>> initClusterIterators(CommandContext ctx) {
    var db = ctx.getDatabase();
    Collection<String> targetClusterNames = new HashSet<>();

    if ((this.classes == null || this.classes.isEmpty())
        && (this.clusters == null || this.clusters.isEmpty())) {
      targetClusterNames.addAll(ctx.getDatabase().getClusterNames());
    } else {
      if (this.clusters != null) {
        for (SQLCluster c : this.clusters) {
          if (c.getClusterName() != null) {
            targetClusterNames.add(c.getClusterName());
          } else {
            String clusterName = db.getClusterNameById(c.getClusterNumber());
            if (clusterName == null) {
              throw new CommandExecutionException("Cluster not found: " + c.getClusterNumber());
            }
            targetClusterNames.add(clusterName);
          }
        }
        Schema schema = db.getMetadata().getImmutableSchemaSnapshot();
        assert this.classes != null;
        for (SQLIdentifier className : this.classes) {
          SchemaClass clazz = schema.getClass(className.getStringValue());
          if (clazz == null) {
            throw new CommandExecutionException("Class not found: " + className);
          }
          for (int clusterId : clazz.getPolymorphicClusterIds()) {
            targetClusterNames.add(db.getClusterNameById(clusterId));
          }
        }
      }
    }

    return targetClusterNames.stream()
        .map(clusterName -> new RecordIteratorCluster<>(db, db.getClusterIdByName(clusterName)))
        .collect(Collectors.toList());
  }

  private Set<RID> fetchRidsToFind(CommandContext ctx) {
    Set<RID> ridsToFind = new HashSet<>();

    ExecutionStepInternal prevStep = prev;
    assert prevStep != null;
    ExecutionStream nextSlot = prevStep.start(ctx);
    while (nextSlot.hasNext(ctx)) {
      Result nextRes = nextSlot.next(ctx);
      if (nextRes.isEntity()) {
        ridsToFind.add(nextRes.toEntity().getIdentity());
      }
    }
    nextSlot.close(ctx);
    return ridsToFind;
  }

  private static List<String> checkObject(
      final Set<RID> iSourceRIDs, final Object value, final DBRecord iRootObject, String prefix) {
    if (value instanceof Result) {
      return checkRoot(iSourceRIDs, (Result) value, iRootObject, prefix).stream()
          .map(y -> value + "." + y)
          .collect(Collectors.toList());
    } else if (value instanceof Identifiable) {
      return checkRecord(iSourceRIDs, (Identifiable) value, iRootObject, prefix).stream()
          .map(y -> value + "." + y)
          .collect(Collectors.toList());
    } else if (value instanceof Collection<?>) {
      return checkCollection(iSourceRIDs, (Collection<?>) value, iRootObject, prefix).stream()
          .map(y -> value + "." + y)
          .collect(Collectors.toList());
    } else if (value instanceof Map<?, ?>) {
      return checkMap(iSourceRIDs, (Map<?, ?>) value, iRootObject, prefix).stream()
          .map(y -> value + "." + y)
          .collect(Collectors.toList());
    } else {
      return new ArrayList<>();
    }
  }

  private static List<String> checkCollection(
      final Set<RID> iSourceRIDs,
      final Collection<?> values,
      final DBRecord iRootObject,
      String prefix) {
    final Iterator<?> it = values.iterator();
    List<String> result = new ArrayList<>();
    while (it.hasNext()) {
      result.addAll(checkObject(iSourceRIDs, it.next(), iRootObject, prefix));
    }
    return result;
  }

  private static List<String> checkMap(
      final Set<RID> iSourceRIDs,
      final Map<?, ?> values,
      final DBRecord iRootObject,
      String prefix) {
    final Iterator<?> it;
    if (values instanceof LinkMap) {
      it = ((LinkMap) values).rawIterator();
    } else {
      it = values.values().iterator();
    }
    List<String> result = new ArrayList<>();
    while (it.hasNext()) {
      result.addAll(checkObject(iSourceRIDs, it.next(), iRootObject, prefix));
    }
    return result;
  }

  private static List<String> checkRecord(
      final Set<RID> iSourceRIDs,
      final Identifiable value,
      final DBRecord iRootObject,
      String prefix) {
    List<String> result = new ArrayList<>();
    if (iSourceRIDs.contains(value.getIdentity())) {
      result.add(prefix);
    } else if (!((RecordId) value.getIdentity()).isValid()
        && value.getRecord() instanceof EntityImpl) {
      // embedded document
      EntityImpl entity = value.getRecord();
      for (String fieldName : entity.fieldNames()) {
        Object fieldValue = entity.field(fieldName);
        result.addAll(checkObject(iSourceRIDs, fieldValue, iRootObject, prefix + "." + fieldName));
      }
    }
    return result;
  }

  private static List<String> checkRoot(
      final Set<RID> iSourceRIDs, final Result value, final DBRecord iRootObject,
      String prefix) {
    List<String> result = new ArrayList<>();
    for (String fieldName : value.getPropertyNames()) {
      Object fieldValue = value.getProperty(fieldName);
      result.addAll(checkObject(iSourceRIDs, fieldValue, iRootObject, prefix + "." + fieldName));
    }
    return result;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = ExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ FIND REFERENCES\n");
    result.append(spaces);

    if ((this.classes == null || this.classes.isEmpty())
        && (this.clusters == null || this.clusters.isEmpty())) {
      result.append("  (all db)");
    } else {
      if (this.classes != null && !this.classes.isEmpty()) {
        result.append("  classes: ").append(this.classes);
      }
      if (this.clusters != null && !this.clusters.isEmpty()) {
        result.append("  classes: ").append(this.clusters);
      }
    }
    return result.toString();
  }
}
