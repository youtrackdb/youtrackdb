/*
 *
 *
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
package com.jetbrains.youtrack.db.internal.core.metadata;

import com.jetbrains.youtrack.db.internal.core.index.OIndexManager;
import com.jetbrains.youtrack.db.internal.core.index.OIndexManagerAbstract;
import com.jetbrains.youtrack.db.internal.core.metadata.function.OFunction;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTImmutableSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OIdentity;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORole;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OSecurity;
import com.jetbrains.youtrack.db.internal.core.metadata.security.YTUser;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

/**
 * Internal interface to manage metadata snapshots.
 */
public interface OMetadataInternal extends OMetadata {

  Set<String> SYSTEM_CLUSTER =
      Collections.unmodifiableSet(
          new HashSet<String>(
              Arrays.asList(
                  YTUser.CLASS_NAME.toLowerCase(Locale.ENGLISH),
                  ORole.CLASS_NAME.toLowerCase(Locale.ENGLISH),
                  OIdentity.CLASS_NAME.toLowerCase(Locale.ENGLISH),
                  OSecurity.RESTRICTED_CLASSNAME.toLowerCase(Locale.ENGLISH),
                  OFunction.CLASS_NAME.toLowerCase(Locale.ENGLISH),
                  "internal")));

  void makeThreadLocalSchemaSnapshot();

  void clearThreadLocalSchemaSnapshot();

  YTImmutableSchema getImmutableSchemaSnapshot();

  OIndexManagerAbstract getIndexManagerInternal();

  @Deprecated
  void load();

  @Deprecated
  void create() throws IOException;

  /**
   * @deprecated Manual indexes are deprecated and will be removed
   */
  @Deprecated
  OIndexManager getIndexManager();

  @Deprecated
  int getSchemaClusterId();

  /**
   * Reloads the internal objects.
   */
  void reload();

  /**
   * Closes internal objects
   */
  @Deprecated
  void close();
}
