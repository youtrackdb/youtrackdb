package com.jetbrains.youtrack.db.auto;

import static org.testng.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;

public class IndexTestingHelper {

  private final DatabaseSessionInternal database;
  private final boolean newEngine;

  public IndexTestingHelper(DatabaseSessionInternal database, boolean newEngine) {
    this.database = database;
    this.newEngine = newEngine;
  }

  public List<Entity> runAndCheckIndex(int indexCount, int paramCount, int keyCount, String query,
      Object... params) {

    if (newEngine) {

      final var explainQuery = "EXPLAIN " + query;
      final var explainResult = database.query(explainQuery, params).getExecutionPlan()
          .orElseThrow();
      final var iu = BaseDBTest.indexesUsed(explainResult);
      assertEquals(iu, indexCount);

      return database.query(query, params).toEntityList();
    } else {

      final var result = withRecording(() ->
          database.command(new SQLSynchQuery<EntityImpl>(query))
              .<List<Entity>>execute(database, params)
      );

      assertEquals(result.events.size(), indexCount);
      if (indexCount > 0) {
        assertEquals(result.events.getFirst().getString("databaseName"), database.getName());
        if (paramCount >= 0) {
          assertEquals(result.events.getFirst().getInt("paramCount"), paramCount);
        }
        if (keyCount >= 0) {
          assertEquals(result.events.getFirst().getInt("keyCount"), keyCount);
        }
      }

      return result.value;
    }
  }

  public List<Entity> queryWithIndex(int paramCount, String query, Object... params) {
    return runAndCheckIndex(1, paramCount, -1, query, params);
  }

  public List<Entity> queryWithIndex(int paramCount, int keyCount, String query, Object... params) {
    return runAndCheckIndex(1, paramCount, keyCount, query, params);
  }

  public List<Entity> queryWithMultipleIndexes(int indicesCount, String query, Object... params) {
    return runAndCheckIndex(indicesCount, -1, -1, query, params);
  }

  public List<Entity> queryWithoutIndex(String query, Object... params) {
    return runAndCheckIndex(0, -1, -1, query, params);
  }

  public <T> RecordingResult<T> withRecording(Supplier<T> dbBlock) {

    final List<RecordedEvent> events = new ArrayList<>();
    try (var stream = new RecordingStream()) {
      stream.enable("com.jetbrains.youtrack.db.core.QueryIndexUsed");
      stream.onEvent("com.jetbrains.youtrack.db.core.QueryIndexUsed", events::add);
      stream.startAsync();
      final T result = dbBlock.get();
      stream.stop();

      return new RecordingResult<>(result, events);
    }
  }

  public static class RecordingResult<T> {

    final T value;
    final List<RecordedEvent> events;

    private RecordingResult(T result, List<RecordedEvent> events) {
      this.value = result;
      this.events = events;
    }
  }
}
