/*
 *
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
package com.jetbrains.youtrack.db.internal.core.sql.functions.text;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.command.CommandContext;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.security.OSecurityManager;
import com.jetbrains.youtrack.db.internal.core.sql.method.misc.OAbstractSQLMethod;
import java.io.UnsupportedEncodingException;
import java.security.NoSuchAlgorithmException;

/**
 * Hash a string supporting multiple algorithm, all those supported by JVM
 */
public class OSQLMethodHash extends OAbstractSQLMethod {

  public static final String NAME = "hash";

  public OSQLMethodHash() {
    super(NAME, 0, 1);
  }

  @Override
  public String getSyntax() {
    return "hash([<algorithm>])";
  }

  @Override
  public Object execute(
      final Object iThis,
      final YTIdentifiable iCurrentRecord,
      final CommandContext iContext,
      final Object ioResult,
      final Object[] iParams) {
    if (iThis == null) {
      return null;
    }

    final String algorithm =
        iParams.length > 0 ? iParams[0].toString() : OSecurityManager.HASH_ALGORITHM;
    try {
      return OSecurityManager.createHash(iThis.toString(), algorithm);

    } catch (NoSuchAlgorithmException e) {
      throw YTException.wrapException(
          new YTCommandExecutionException("hash(): algorithm '" + algorithm + "' is not supported"),
          e);
    } catch (UnsupportedEncodingException e) {
      throw YTException.wrapException(
          new YTCommandExecutionException("hash(): encoding 'UTF-8' is not supported"), e);
    }
  }
}
