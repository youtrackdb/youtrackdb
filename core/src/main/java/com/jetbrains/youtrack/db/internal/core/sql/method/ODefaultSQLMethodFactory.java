/*
 * Copyright 2013 Geomatys.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.internal.core.sql.method;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.sql.functions.coll.OSQLMethodMultiValue;
import com.jetbrains.youtrack.db.internal.core.sql.functions.conversion.OSQLMethodAsDate;
import com.jetbrains.youtrack.db.internal.core.sql.functions.conversion.OSQLMethodAsDateTime;
import com.jetbrains.youtrack.db.internal.core.sql.functions.conversion.OSQLMethodAsDecimal;
import com.jetbrains.youtrack.db.internal.core.sql.functions.conversion.OSQLMethodBeforeUpdate;
import com.jetbrains.youtrack.db.internal.core.sql.functions.conversion.OSQLMethodConvert;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLMethodExclude;
import com.jetbrains.youtrack.db.internal.core.sql.functions.misc.OSQLMethodInclude;
import com.jetbrains.youtrack.db.internal.core.sql.functions.text.OSQLMethodAppend;
import com.jetbrains.youtrack.db.internal.core.sql.functions.text.OSQLMethodFromJSON;
import com.jetbrains.youtrack.db.internal.core.sql.functions.text.OSQLMethodHash;
import com.jetbrains.youtrack.db.internal.core.sql.functions.text.OSQLMethodLength;
import com.jetbrains.youtrack.db.internal.core.sql.functions.text.OSQLMethodReplace;
import com.jetbrains.youtrack.db.internal.core.sql.functions.text.OSQLMethodRight;
import com.jetbrains.youtrack.db.internal.core.sql.functions.text.OSQLMethodSubString;
import com.jetbrains.youtrack.db.internal.core.sql.functions.text.OSQLMethodToJSON;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodAsBoolean;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodAsFloat;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodAsInteger;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodAsList;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodAsLong;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodAsMap;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodAsSet;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodAsString;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodField;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodFormat;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodFunctionDelegate;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodIndexOf;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodJavaType;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodKeys;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodLastIndexOf;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodNormalize;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodPrefix;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodRemove;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodRemoveAll;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodSize;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodSplit;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodToLowerCase;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodToUpperCase;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodTrim;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodType;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OSQLMethodValues;
import com.jetbrains.youtrack.db.internal.core.sql.method.sequence.OSQLMethodCurrent;
import com.jetbrains.youtrack.db.internal.core.sql.method.sequence.OSQLMethodNext;
import com.jetbrains.youtrack.db.internal.core.sql.method.sequence.OSQLMethodReset;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Default method factory.
 */
public class ODefaultSQLMethodFactory implements OSQLMethodFactory {

  private final Map<String, Object> methods = new HashMap<String, Object>();

  public ODefaultSQLMethodFactory() {
    register(OSQLMethodAppend.NAME, new OSQLMethodAppend());
    register(OSQLMethodAsBoolean.NAME, new OSQLMethodAsBoolean());
    register(OSQLMethodAsDate.NAME, new OSQLMethodAsDate());
    register(OSQLMethodAsDateTime.NAME, new OSQLMethodAsDateTime());
    register(OSQLMethodAsDecimal.NAME, new OSQLMethodAsDecimal());
    register(OSQLMethodAsFloat.NAME, new OSQLMethodAsFloat());
    register(OSQLMethodAsInteger.NAME, new OSQLMethodAsInteger());
    register(OSQLMethodAsList.NAME, new OSQLMethodAsList());
    register(OSQLMethodAsLong.NAME, new OSQLMethodAsLong());
    register(OSQLMethodAsMap.NAME, new OSQLMethodAsMap());
    register(OSQLMethodAsSet.NAME, new OSQLMethodAsSet());
    register(OSQLMethodAsString.NAME, new OSQLMethodAsString());
    register(OSQLMethodCharAt.NAME, new OSQLMethodCharAt());
    register(OSQLMethodConvert.NAME, new OSQLMethodConvert());
    register(OSQLMethodExclude.NAME, new OSQLMethodExclude());
    register(OSQLMethodField.NAME, new OSQLMethodField());
    register(OSQLMethodFormat.NAME, new OSQLMethodFormat());
    register(OSQLMethodFromJSON.NAME, new OSQLMethodFromJSON());
    register(OSQLMethodFunctionDelegate.NAME, OSQLMethodFunctionDelegate.class);
    register(OSQLMethodHash.NAME, new OSQLMethodHash());
    register(OSQLMethodInclude.NAME, new OSQLMethodInclude());
    register(OSQLMethodIndexOf.NAME, new OSQLMethodIndexOf());
    register(OSQLMethodJavaType.NAME, new OSQLMethodJavaType());
    register(OSQLMethodKeys.NAME, new OSQLMethodKeys());
    register(OSQLMethodLastIndexOf.NAME, new OSQLMethodLastIndexOf());
    register(OSQLMethodLeft.NAME, new OSQLMethodLeft());
    register(OSQLMethodLength.NAME, new OSQLMethodLength());
    register(OSQLMethodMultiValue.NAME, new OSQLMethodMultiValue());
    register(OSQLMethodNormalize.NAME, new OSQLMethodNormalize());
    register(OSQLMethodPrefix.NAME, new OSQLMethodPrefix());
    register(OSQLMethodRemove.NAME, new OSQLMethodRemove());
    register(OSQLMethodRemoveAll.NAME, new OSQLMethodRemoveAll());
    register(OSQLMethodReplace.NAME, new OSQLMethodReplace());
    register(OSQLMethodRight.NAME, new OSQLMethodRight());
    register(OSQLMethodSize.NAME, new OSQLMethodSize());
    register(OSQLMethodSplit.NAME, new OSQLMethodSplit());
    register(OSQLMethodToLowerCase.NAME, new OSQLMethodToLowerCase());
    register(OSQLMethodToUpperCase.NAME, new OSQLMethodToUpperCase());
    register(OSQLMethodTrim.NAME, new OSQLMethodTrim());
    register(OSQLMethodType.NAME, new OSQLMethodType());
    register(OSQLMethodSubString.NAME, new OSQLMethodSubString());
    register(OSQLMethodToJSON.NAME, new OSQLMethodToJSON());
    register(OSQLMethodValues.NAME, new OSQLMethodValues());
    register(OSQLMethodBeforeUpdate.NAME, new OSQLMethodBeforeUpdate());

    // SEQUENCE
    register(OSQLMethodCurrent.NAME, new OSQLMethodCurrent());
    register(OSQLMethodNext.NAME, new OSQLMethodNext());
    register(OSQLMethodReset.NAME, new OSQLMethodReset());
  }

  public void register(final String iName, final Object iImplementation) {
    methods.put(iName.toLowerCase(Locale.ENGLISH), iImplementation);
  }

  @Override
  public boolean hasMethod(final String iName) {
    return methods.containsKey(iName.toLowerCase(Locale.ENGLISH));
  }

  @Override
  public Set<String> getMethodNames() {
    return methods.keySet();
  }

  @Override
  public OSQLMethod createMethod(final String name) throws YTCommandExecutionException {
    final Object m = methods.get(name);
    final OSQLMethod method;

    if (m instanceof Class<?>) {
      try {
        method = (OSQLMethod) ((Class<?>) m).newInstance();
      } catch (Exception e) {
        throw YTException.wrapException(
            new YTCommandExecutionException("Cannot create SQL method: " + m), e);
      }
    } else {
      method = (OSQLMethod) m;
    }

    if (method == null) {
      throw new YTCommandExecutionException("Unknown method name: " + name);
    }

    return method;
  }
}
