/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.jetbrains.youtrack.db.internal.core.metadata.function;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.CallableFunction;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClassInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Pattern;

/**
 * Manages stored functions.
 */
public class FunctionLibraryImpl {

  public static final String CLASSNAME = "OFunction";
  protected final ConcurrentHashMap<String, Function> functions = new ConcurrentHashMap<>();
  private final AtomicBoolean needReload = new AtomicBoolean(false);

  public FunctionLibraryImpl() {
  }

  public static void create(DatabaseSessionInternal db) {
    init(db);
  }

  public void load(DatabaseSessionInternal db) {
    // COPY CALLBACK IN RAM
    final Map<String, CallableFunction<Object, Map<Object, Object>>> callbacks =
        new HashMap<String, CallableFunction<Object, Map<Object, Object>>>();
    for (var entry : functions.entrySet()) {
      if (entry.getValue().getCallback() != null) {
        callbacks.put(entry.getKey(), entry.getValue().getCallback());
      }
    }

    functions.clear();

    // LOAD ALL THE FUNCTIONS IN MEMORY
    if (db.getMetadata().getImmutableSchemaSnapshot().existsClass("OFunction")) {
      try (var result = db.query("select from OFunction order by name")) {
        while (result.hasNext()) {
          var res = result.next();
          var d = (EntityImpl) res.getEntity().orElseThrow();
          // skip the function records which do not contain real data
          if (d.fields() == 0) {
            continue;
          }

          final var f = new Function(db, d);

          // RESTORE CALLBACK IF ANY
          f.setCallback(callbacks.get(f.getName()));

          functions.put(d.field("name").toString().toUpperCase(Locale.ENGLISH), f);
        }
      }
    }
  }

  public void droppedFunction(DatabaseSessionInternal db, EntityImpl function) {
    functions.remove(function.field("name").toString());
    onFunctionsChanged(db);
  }

  public void createdFunction(DatabaseSessionInternal db, EntityImpl function) {
    final var f = new Function(db, function.getIdentity());
    functions.put(function.getProperty("name").toString().toUpperCase(Locale.ENGLISH), f);
    onFunctionsChanged(db);
  }

  public Set<String> getFunctionNames() {
    return Collections.unmodifiableSet(functions.keySet());
  }

  public Function getFunction(DatabaseSessionInternal db, final String iName) {
    reloadIfNeeded(db);
    return functions.get(iName.toUpperCase(Locale.ENGLISH));
  }

  public synchronized Function createFunction(
      DatabaseSessionInternal database, final String iName) {
    init(database);
    reloadIfNeeded(database);

    database.begin();
    final var f = new Function(database).setName(iName);
    try {
      f.save(database);
      functions.put(iName.toUpperCase(Locale.ENGLISH), f);
      database.commit();
    } catch (RecordDuplicatedException ex) {
      LogManager.instance().error(this, "Exception is suppressed, original exception is ", ex);

      //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
      throw BaseException.wrapException(
          new FunctionDuplicatedException("Function with name '" + iName + "' already exist"),
          null, database.getDatabaseName());
    }

    return f;
  }

  public void close() {
    functions.clear();
  }

  protected static void init(final DatabaseSessionInternal db) {
    if (db.getMetadata().getSchema().existsClass("OFunction")) {
      var f = db.getMetadata().getSchema().getClassInternal("OFunction");
      var prop = f.getPropertyInternal(db, "name");

      if (prop.getAllIndexes(db).isEmpty()) {
        prop.createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE);
      }
      return;
    }

    var f = (SchemaClassInternal) db.getMetadata().getSchema().createClass("OFunction");
    var prop = f.createProperty(db, "name", PropertyType.STRING, (PropertyType) null, true);
    prop.createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE);

    f.createProperty(db, "code", PropertyType.STRING, (PropertyType) null, true);
    f.createProperty(db, "language", PropertyType.STRING, (PropertyType) null, true);
    f.createProperty(db, "idempotent", PropertyType.BOOLEAN, (PropertyType) null, true);
    f.createProperty(db, "parameters", PropertyType.EMBEDDEDLIST, PropertyType.STRING, true);
  }

  public synchronized void dropFunction(DatabaseSessionInternal session, Function function) {
    reloadIfNeeded(session);
    var name = function.getName();
    function.delete(session);
    functions.remove(name.toUpperCase(Locale.ENGLISH));
  }

  public synchronized void dropFunction(DatabaseSessionInternal session, String iName) {
    reloadIfNeeded(session);

    session.executeInTx(
        () -> {
          var function = getFunction(session, iName);
          function.delete(session);
          functions.remove(iName.toUpperCase(Locale.ENGLISH));
        });
  }

  public void updatedFunction(DatabaseSessionInternal database, EntityImpl function) {
    reloadIfNeeded(database);
    var oldName = (String) function.getOriginalValue("name");
    if (oldName != null) {
      functions.remove(oldName.toUpperCase(Locale.ENGLISH));
    }
    CallableFunction<Object, Map<Object, Object>> callBack = null;
    var oldFunction = functions.get(function.getProperty("name").toString());
    if (oldFunction != null) {
      callBack = oldFunction.getCallback();
    }

    final var f = new Function(database, function.getIdentity());
    if (callBack != null) {
      f.setCallback(callBack);
    }

    functions.put(function.getProperty("name").toString().toUpperCase(Locale.ENGLISH), f);
    onFunctionsChanged(database);
  }

  private void reloadIfNeeded(DatabaseSessionInternal database) {
    if (needReload.get()) {
      load(database);
      needReload.set(false);
    }
  }

  private static void onFunctionsChanged(DatabaseSessionInternal database) {
    database.getSharedContext().getYouTrackDB().getScriptManager()
        .close(database.getDatabaseName());
  }

  public synchronized void update() {
    needReload.set(true);
  }

  public static void validateFunctionRecord(EntityImpl entity) throws DatabaseException {
    String name = entity.getProperty("name");
    if (!Pattern.compile("[A-Za-z][A-Za-z0-9_]*").matcher(name).matches()) {
      var session = entity.getBoundedToSession();
      throw new DatabaseException(session != null ? session.getDatabaseName() : null,
          "Invalid function name: " + name);
    }
  }
}
