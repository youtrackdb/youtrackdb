/*
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
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class OSystemRole extends ORole {

  public static final String DB_FILTER = "dbFilter";

  private List<String> dbFilter;

  public List<String> getDbFilter() {
    return dbFilter;
  }

  /**
   * Constructor used in unmarshalling.
   */
  public OSystemRole() {
  }

  public OSystemRole(
      YTDatabaseSessionInternal session, final String iName,
      final ORole iParent,
      final ALLOW_MODES iAllowMode,
      Map<String, OSecurityPolicy> policies) {
    super(session, iName, iParent, iAllowMode, policies);
  }

  /**
   * Create the role by reading the source document.
   */
  public OSystemRole(YTDatabaseSessionInternal session, final YTDocument iSource) {
    super(session, iSource);
  }

  @Override
  public void fromStream(YTDatabaseSessionInternal session, final YTDocument iSource) {
    super.fromStream(session, iSource);

    var document = getDocument(session);
    if (document != null
        && document.containsField(DB_FILTER)
        && document.fieldType(DB_FILTER) == YTType.EMBEDDEDLIST) {
      dbFilter = document.field(DB_FILTER, YTType.EMBEDDEDLIST);
    }
  }
}
