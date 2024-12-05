/*
 * Copyright 2010-2012 henryzhao81-at-gmail.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.core.db.record;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.command.script.YTCommandScriptException;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.YTConfigurationException;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.hook.YTRecordHook;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import java.lang.reflect.Method;
import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

/**
 * Author : henryzhao81@gmail.com Feb 19, 2013
 *
 * <p>Create a class OTriggered which contains 8 additional class attributes, which link to
 * OFunction - beforeCreate - afterCreate - beforeRead - afterRead - beforeUpdate - afterUpdate -
 * beforeDelete - afterDelete
 */
public class OClassTrigger {

  public static final String CLASSNAME = "OTriggered";
  public static final String METHOD_SEPARATOR = ".";

  // Class Level Trigger (class custom attribute)
  public static final String ONBEFORE_CREATED = "onBeforeCreate";
  // Record Level Trigger (property name)
  public static final String PROP_BEFORE_CREATE = ONBEFORE_CREATED;
  public static final String ONAFTER_CREATED = "onAfterCreate";
  public static final String PROP_AFTER_CREATE = ONAFTER_CREATED;
  public static final String ONBEFORE_READ = "onBeforeRead";
  public static final String PROP_BEFORE_READ = ONBEFORE_READ;
  public static final String ONAFTER_READ = "onAfterRead";
  public static final String PROP_AFTER_READ = ONAFTER_READ;
  public static final String ONBEFORE_UPDATED = "onBeforeUpdate";
  public static final String PROP_BEFORE_UPDATE = ONBEFORE_UPDATED;
  public static final String ONAFTER_UPDATED = "onAfterUpdate";
  public static final String PROP_AFTER_UPDATE = ONAFTER_UPDATED;
  public static final String ONBEFORE_DELETE = "onBeforeDelete";
  public static final String PROP_BEFORE_DELETE = ONBEFORE_DELETE;
  public static final String ONAFTER_DELETE = "onAfterDelete";
  public static final String PROP_AFTER_DELETE = ONAFTER_DELETE;

  public static YTRecordHook.RESULT onRecordBeforeCreate(
      final YTEntityImpl iDocument, YTDatabaseSessionInternal database) {
    Object func = checkClzAttribute(iDocument, ONBEFORE_CREATED, database);
    if (func != null) {
      if (func instanceof OFunction) {
        return OClassTrigger.executeFunction(iDocument, (OFunction) func, database);
      } else if (func instanceof Object[]) {
        return OClassTrigger.executeMethod(iDocument, (Object[]) func);
      }
    }
    return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
  }

  public static void onRecordAfterCreate(
      final YTEntityImpl iDocument, YTDatabaseSessionInternal database) {
    Object func = checkClzAttribute(iDocument, ONAFTER_CREATED, database);
    if (func != null) {
      if (func instanceof OFunction) {
        OClassTrigger.executeFunction(iDocument, (OFunction) func, database);
      } else if (func instanceof Object[]) {
        OClassTrigger.executeMethod(iDocument, (Object[]) func);
      }
    }
  }

  public static YTRecordHook.RESULT onRecordBeforeRead(
      final YTEntityImpl iDocument, YTDatabaseSessionInternal database) {
    Object func = checkClzAttribute(iDocument, ONBEFORE_READ, database);
    if (func != null) {
      if (func instanceof OFunction) {
        return OClassTrigger.executeFunction(iDocument, (OFunction) func, database);
      } else if (func instanceof Object[]) {
        return OClassTrigger.executeMethod(iDocument, (Object[]) func);
      }
    }
    return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
  }

  public static void onRecordAfterRead(
      final YTEntityImpl iDocument, YTDatabaseSessionInternal database) {
    Object func = checkClzAttribute(iDocument, ONAFTER_READ, database);
    if (func != null) {
      if (func instanceof OFunction) {
        OClassTrigger.executeFunction(iDocument, (OFunction) func, database);
      } else if (func instanceof Object[]) {
        OClassTrigger.executeMethod(iDocument, (Object[]) func);
      }
    }
  }

  public static YTRecordHook.RESULT onRecordBeforeUpdate(
      final YTEntityImpl iDocument, YTDatabaseSessionInternal database) {
    Object func = checkClzAttribute(iDocument, ONBEFORE_UPDATED, database);
    if (func != null) {
      if (func instanceof OFunction) {
        return OClassTrigger.executeFunction(iDocument, (OFunction) func, database);
      } else if (func instanceof Object[]) {
        return OClassTrigger.executeMethod(iDocument, (Object[]) func);
      }
    }
    return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
  }

  public static void onRecordAfterUpdate(
      final YTEntityImpl iDocument, YTDatabaseSessionInternal database) {
    Object func = checkClzAttribute(iDocument, ONAFTER_UPDATED, database);
    if (func != null) {
      if (func instanceof OFunction) {
        OClassTrigger.executeFunction(iDocument, (OFunction) func, database);
      } else if (func instanceof Object[]) {
        OClassTrigger.executeMethod(iDocument, (Object[]) func);
      }
    }
  }

  public static YTRecordHook.RESULT onRecordBeforeDelete(
      final YTEntityImpl iDocument, YTDatabaseSessionInternal database) {
    Object func = checkClzAttribute(iDocument, ONBEFORE_DELETE, database);
    if (func != null) {
      if (func instanceof OFunction) {
        return OClassTrigger.executeFunction(iDocument, (OFunction) func, database);
      } else if (func instanceof Object[]) {
        return OClassTrigger.executeMethod(iDocument, (Object[]) func);
      }
    }
    return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
  }

  public static void onRecordAfterDelete(
      final YTEntityImpl iDocument, YTDatabaseSessionInternal database) {
    Object func = checkClzAttribute(iDocument, ONAFTER_DELETE, database);
    if (func != null) {
      if (func instanceof OFunction) {
        OClassTrigger.executeFunction(iDocument, (OFunction) func, database);
      } else if (func instanceof Object[]) {
        OClassTrigger.executeMethod(iDocument, (Object[]) func);
      }
    }
  }

  private static Object checkClzAttribute(
      final YTEntityImpl iDocument, String attr, YTDatabaseSessionInternal database) {
    final YTImmutableClass clz = ODocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (clz != null && clz.isTriggered()) {
      OFunction func = null;
      String fieldName = clz.getCustom(attr);
      YTClass superClz = clz.getSuperClass();
      while (fieldName == null || fieldName.length() == 0) {
        if (superClz == null || superClz.getName().equals(CLASSNAME)) {
          break;
        }
        fieldName = superClz.getCustom(attr);
        superClz = superClz.getSuperClass();
      }
      if (fieldName != null && fieldName.length() > 0) {
        // check if it is reflection or not
        final Object[] clzMethod = OClassTrigger.checkMethod(fieldName);
        if (clzMethod != null) {
          return clzMethod;
        }
        func = database.getMetadata().getFunctionLibrary().getFunction(fieldName);
        if (func == null) { // check if it is rid
          if (OStringSerializerHelper.contains(fieldName, YTRID.SEPARATOR)) {
            try {
              try {
                YTEntityImpl funcDoc = database.load(new YTRecordId(fieldName));
                func =
                    database.getMetadata().getFunctionLibrary().getFunction(funcDoc.field("name"));
              } catch (YTRecordNotFoundException rnf) {
                // ignore
              }
            } catch (Exception ex) {
              OLogManager.instance().error(OClassTrigger.class, "illegal record id : ", ex);
            }
          }
        }
      } else {
        final Object funcProp = iDocument.field(attr);
        if (funcProp != null) {
          final String funcName;
          if (funcProp instanceof YTEntityImpl) {
            funcName = ((YTEntityImpl) funcProp).field("name");
          } else {
            funcName = funcProp.toString();
          }
          func = database.getMetadata().getFunctionLibrary().getFunction(funcName);
        }
      }
      return func;
    }
    return null;
  }

  private static Object[] checkMethod(String fieldName) {
    String clzName = null;
    String methodName = null;
    if (fieldName.contains(METHOD_SEPARATOR)) {
      clzName = fieldName.substring(0, fieldName.lastIndexOf(METHOD_SEPARATOR));
      methodName = fieldName.substring(fieldName.lastIndexOf(METHOD_SEPARATOR) + 1);
    }
    if (clzName == null || methodName == null) {
      return null;
    }
    try {
      Class clz = ClassLoader.getSystemClassLoader().loadClass(clzName);
      Method method = clz.getMethod(methodName, YTEntityImpl.class);
      return new Object[]{clz, method};
    } catch (Exception ex) {
      OLogManager.instance()
          .error(
              OClassTrigger.class, "illegal class or method : " + clzName + "/" + methodName, ex);
      return null;
    }
  }

  private static YTRecordHook.RESULT executeMethod(
      final YTEntityImpl iDocument, final Object[] clzMethod) {
    if (clzMethod[0] instanceof Class clz && clzMethod[1] instanceof Method method) {
      String result = null;
      try {
        result = (String) method.invoke(clz.newInstance(), iDocument);
      } catch (Exception ex) {
        throw YTException.wrapException(
            new YTDatabaseException("Failed to invoke method " + method.getName()), ex);
      }
      if (result == null) {
        return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
      }
      return YTRecordHook.RESULT.valueOf(result);
    }
    return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
  }

  private static YTRecordHook.RESULT executeFunction(
      final YTEntityImpl iDocument, final OFunction func, YTDatabaseSessionInternal database) {
    if (func == null) {
      return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
    }

    final OScriptManager scriptManager =
        database.getSharedContext().getYouTrackDB().getScriptManager();

    final ScriptEngine scriptEngine =
        scriptManager.acquireDatabaseEngine(database.getName(), func.getLanguage(database));
    try {
      final Bindings binding = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);

      scriptManager.bind(scriptEngine, binding, database, null, null);
      binding.put("doc", iDocument);

      String result = null;
      try {
        if (func.getLanguage(database) == null) {
          throw new YTConfigurationException(
              "Database function '" + func.getName(database) + "' has no language");
        }
        final String funcStr = scriptManager.getFunctionDefinition(database, func);
        if (funcStr != null) {
          try {
            scriptEngine.eval(funcStr);
          } catch (ScriptException e) {
            scriptManager.throwErrorMessage(e, funcStr);
          }
        }
        if (scriptEngine instanceof Invocable invocableEngine) {
          Object[] empty = OCommonConst.EMPTY_OBJECT_ARRAY;
          result = (String) invocableEngine.invokeFunction(func.getName(database), empty);
        }
      } catch (ScriptException e) {
        throw YTException.wrapException(
            new YTCommandScriptException(
                "Error on execution of the script", func.getName(database), e.getColumnNumber()),
            e);
      } catch (NoSuchMethodException e) {
        throw YTException.wrapException(
            new YTCommandScriptException("Error on execution of the script", func.getName(database),
                0), e);
      } catch (YTCommandScriptException e) {
        // PASS THROUGH
        throw e;

      } finally {
        scriptManager.unbind(scriptEngine, binding, null, null);
      }
      if (result == null) {
        return YTRecordHook.RESULT.RECORD_NOT_CHANGED;
      }
      return YTRecordHook.RESULT.valueOf(result);

    } finally {
      scriptManager.releaseDatabaseEngine(func.getLanguage(database), database.getName(),
          scriptEngine);
    }
  }
}
