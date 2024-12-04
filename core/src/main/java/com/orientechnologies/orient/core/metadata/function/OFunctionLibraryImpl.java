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
package com.orientechnologies.orient.core.metadata.function;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OMetadataUpdateListener;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.YTRecordDuplicatedException;
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
public class OFunctionLibraryImpl {

  public static final String CLASSNAME = "OFunction";
  protected final Map<String, OFunction> functions = new ConcurrentHashMap<String, OFunction>();
  private final AtomicBoolean needReload = new AtomicBoolean(false);

  public OFunctionLibraryImpl() {
  }

  public void create(YTDatabaseSessionInternal db) {
    init(db);
  }

  public void load() {
    throw new UnsupportedOperationException();
  }

  public void load(YTDatabaseSessionInternal db) {
    // COPY CALLBACK IN RAM
    final Map<String, OCallable<Object, Map<Object, Object>>> callbacks =
        new HashMap<String, OCallable<Object, Map<Object, Object>>>();
    for (Map.Entry<String, OFunction> entry : functions.entrySet()) {
      if (entry.getValue().getCallback() != null) {
        callbacks.put(entry.getKey(), entry.getValue().getCallback());
      }
    }

    functions.clear();

    // LOAD ALL THE FUNCTIONS IN MEMORY
    if (db.getMetadata().getImmutableSchemaSnapshot().existsClass("OFunction")) {
      try (OResultSet result = db.query("select from OFunction order by name")) {
        while (result.hasNext()) {
          OResult res = result.next();
          YTDocument d = (YTDocument) res.getElement().get();
          // skip the function records which do not contain real data
          if (d.fields() == 0) {
            continue;
          }

          final OFunction f = new OFunction(d);

          // RESTORE CALLBACK IF ANY
          f.setCallback(callbacks.get(f.getName(db)));

          functions.put(d.field("name").toString().toUpperCase(Locale.ENGLISH), f);
        }
      }
    }
  }

  public void droppedFunction(YTDocument function) {
    functions.remove(function.field("name").toString());
    onFunctionsChanged(ODatabaseRecordThreadLocal.instance().get());
  }

  public void createdFunction(YTDocument function) {
    YTDocument metadataCopy = function.copy();
    final OFunction f = new OFunction(metadataCopy);
    functions.put(metadataCopy.field("name").toString().toUpperCase(Locale.ENGLISH), f);
    onFunctionsChanged(ODatabaseRecordThreadLocal.instance().get());
  }

  public Set<String> getFunctionNames() {
    return Collections.unmodifiableSet(functions.keySet());
  }

  public OFunction getFunction(final String iName) {
    reloadIfNeeded(ODatabaseRecordThreadLocal.instance().get());
    return functions.get(iName.toUpperCase(Locale.ENGLISH));
  }

  public OFunction createFunction(final String iName) {
    throw new UnsupportedOperationException("Use Create function with database on internal api");
  }

  public synchronized OFunction createFunction(
      YTDatabaseSessionInternal database, final String iName) {
    init(database);
    reloadIfNeeded(ODatabaseRecordThreadLocal.instance().get());

    database.begin();
    final OFunction f = new OFunction(database).setName(database, iName);
    try {
      f.save(database);
      functions.put(iName.toUpperCase(Locale.ENGLISH), f);
      database.commit();
    } catch (YTRecordDuplicatedException ex) {
      OLogManager.instance().error(this, "Exception is suppressed, original exception is ", ex);

      //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
      throw YTException.wrapException(
          new YTFunctionDuplicatedException("Function with name '" + iName + "' already exist"),
          null);
    }

    return f;
  }

  public void close() {
    functions.clear();
  }

  protected void init(final YTDatabaseSessionInternal db) {
    if (db.getMetadata().getSchema().existsClass("OFunction")) {
      final YTClass f = db.getMetadata().getSchema().getClass("OFunction");
      YTProperty prop = f.getProperty("name");
      if (prop.getAllIndexes(db).isEmpty()) {
        prop.createIndex(db, YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
      }
      return;
    }

    final YTClass f = db.getMetadata().getSchema().createClass("OFunction");
    YTProperty prop = f.createProperty(db, "name", YTType.STRING, (YTType) null, true);
    prop.createIndex(db, YTClass.INDEX_TYPE.UNIQUE_HASH_INDEX);
    f.createProperty(db, "code", YTType.STRING, (YTType) null, true);
    f.createProperty(db, "language", YTType.STRING, (YTType) null, true);
    f.createProperty(db, "idempotent", YTType.BOOLEAN, (YTType) null, true);
    f.createProperty(db, "parameters", YTType.EMBEDDEDLIST, YTType.STRING, true);
  }

  public synchronized void dropFunction(YTDatabaseSessionInternal session, OFunction function) {
    reloadIfNeeded(session);
    String name = function.getName(session);
    YTDocument doc = function.getDocument(session);
    doc.delete();
    functions.remove(name.toUpperCase(Locale.ENGLISH));
  }

  public synchronized void dropFunction(YTDatabaseSessionInternal session, String iName) {
    reloadIfNeeded(session);

    session.executeInTx(
        () -> {
          OFunction function = getFunction(iName);
          YTDocument doc = function.getDocument(session);
          doc.delete();
          functions.remove(iName.toUpperCase(Locale.ENGLISH));
        });
  }

  public void updatedFunction(YTDocument function) {
    YTDatabaseSessionInternal database = ODatabaseRecordThreadLocal.instance().get();
    reloadIfNeeded(database);
    String oldName = (String) function.getOriginalValue("name");
    if (oldName != null) {
      functions.remove(oldName.toUpperCase(Locale.ENGLISH));
    }
    YTDocument metadataCopy = function.copy();
    OCallable<Object, Map<Object, Object>> callBack = null;
    OFunction oldFunction = functions.get(metadataCopy.field("name").toString());
    if (oldFunction != null) {
      callBack = oldFunction.getCallback();
    }
    final OFunction f = new OFunction(metadataCopy);
    if (callBack != null) {
      f.setCallback(callBack);
    }
    functions.put(metadataCopy.field("name").toString().toUpperCase(Locale.ENGLISH), f);
    onFunctionsChanged(database);
  }

  private void reloadIfNeeded(YTDatabaseSessionInternal database) {
    if (needReload.get()) {
      load(database);
      needReload.set(false);
    }
  }

  private void onFunctionsChanged(YTDatabaseSessionInternal database) {
    for (OMetadataUpdateListener listener : database.getSharedContext().browseListeners()) {
      listener.onFunctionLibraryUpdate(database, database.getName());
    }
    database.getSharedContext().getYouTrackDB().getScriptManager().close(database.getName());
  }

  public synchronized void update() {
    needReload.set(true);
  }

  public static void validateFunctionRecord(YTDocument doc) throws YTDatabaseException {
    String name = doc.getProperty("name");
    if (!Pattern.compile("[A-Za-z][A-Za-z0-9_]*").matcher(name).matches()) {
      throw new YTDatabaseException("Invalid function name: " + name);
    }
  }
}
