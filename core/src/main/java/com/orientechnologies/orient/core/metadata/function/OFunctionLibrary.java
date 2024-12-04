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

import com.orientechnologies.orient.core.db.YTDatabaseSession;
import java.util.Set;

/**
 * Manages stored functions.
 */
public interface OFunctionLibrary {

  Set<String> getFunctionNames();

  OFunction getFunction(String iName);

  OFunction createFunction(String iName);

  void dropFunction(YTDatabaseSession session, String iName);

  void dropFunction(YTDatabaseSession session, OFunction function);

  @Deprecated
  void create();

  @Deprecated
  void load();

  @Deprecated
  void close();
}
