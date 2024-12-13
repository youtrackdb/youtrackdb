package com.orientechnologies.orient.test.database.auto.hooks;

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.record.RecordHook;
import com.jetbrains.youtrack.db.api.record.RecordHookAbstract;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class BrokenMapHook extends RecordHookAbstract implements RecordHook {

  private final DatabaseSessionInternal database;

  public BrokenMapHook() {
    this.database = DatabaseRecordThreadLocal.instance().get();
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.BOTH;
  }

  public RESULT onRecordBeforeCreate(Record record) {
    Date now = new Date();
    Entity element = (Entity) record;

    if (element.getProperty("myMap") != null) {
      HashMap<String, Object> myMap = new HashMap<>(element.getProperty("myMap"));

      String newDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(now);

      myMap.replaceAll((k, v) -> newDate);

      element.setProperty("myMap", myMap);
    }

    return RESULT.RECORD_CHANGED;
  }

  public RESULT onRecordBeforeUpdate(Record newRecord) {
    Entity newElement = (Entity) newRecord;
    try {
      Entity oldElement = database.load(newElement.getIdentity());

      var newPropertyNames = newElement.getPropertyNames();
      var oldPropertyNames = oldElement.getPropertyNames();

      if (newPropertyNames.contains("myMap") && oldPropertyNames.contains("myMap")) {
        HashMap<String, Object> newFieldValue = newElement.getProperty("myMap");
        HashMap<String, Object> oldFieldValue = new HashMap<>(oldElement.getProperty("myMap"));

        String newDate =
            LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));

        Set<String> newKeys = new HashSet(newFieldValue.keySet());

        newKeys.forEach(
            k -> {
              newFieldValue.remove(k);
              newFieldValue.put(k, newDate);
            });

        oldFieldValue.forEach(
            (k, v) -> {
              if (!newFieldValue.containsKey(k)) {
                newFieldValue.put(k, v);
              }
            });
      }
      return RESULT.RECORD_CHANGED;
    } catch (RecordNotFoundException e) {
      return RESULT.RECORD_NOT_CHANGED;
    }
  }
}
