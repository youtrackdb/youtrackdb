package com.orientechnologies.orient.test.database.auto.hooks;

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.hook.YTRecordHook;
import com.orientechnologies.orient.core.hook.YTRecordHookAbstract;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class BrokenMapHook extends YTRecordHookAbstract implements YTRecordHook {

  private final YTDatabaseSessionInternal database;

  public BrokenMapHook() {
    this.database = ODatabaseRecordThreadLocal.instance().get();
  }

  @Override
  public DISTRIBUTED_EXECUTION_MODE getDistributedExecutionMode() {
    return DISTRIBUTED_EXECUTION_MODE.BOTH;
  }

  public RESULT onRecordBeforeCreate(YTRecord record) {
    Date now = new Date();
    YTEntity element = (YTEntity) record;

    if (element.getProperty("myMap") != null) {
      HashMap<String, Object> myMap = new HashMap<>(element.getProperty("myMap"));

      String newDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(now);

      myMap.replaceAll((k, v) -> newDate);

      element.setProperty("myMap", myMap);
    }

    return RESULT.RECORD_CHANGED;
  }

  public RESULT onRecordBeforeUpdate(YTRecord newRecord) {
    YTEntity newElement = (YTEntity) newRecord;
    try {
      YTEntity oldElement = database.load(newElement.getIdentity());

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
    } catch (YTRecordNotFoundException e) {
      return RESULT.RECORD_NOT_CHANGED;
    }
  }
}
