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
package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.exception.ConfigurationException;
import com.jetbrains.youtrack.db.internal.core.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.DocumentInternal;
import java.util.Set;

/**
 * Checks the access against restricted resources. Restricted resources are those documents of
 * classes that implement ORestricted abstract class.
 */
public class RestrictedAccessHook {

  public static boolean onRecordBeforeCreate(
      final EntityImpl iDocument, DatabaseSessionInternal database) {
    final SchemaImmutableClass cls = DocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (cls != null && cls.isRestricted()) {
      String fieldNames = cls.getCustom(SecurityShared.ONCREATE_FIELD);
      if (fieldNames == null) {
        fieldNames = RestrictedOperation.ALLOW_ALL.getFieldName();
      }
      final String[] fields = fieldNames.split(",");
      String identityType = cls.getCustom(SecurityShared.ONCREATE_IDENTITY_TYPE);
      if (identityType == null) {
        identityType = "user";
      }

      Identifiable identity = null;
      if (identityType.equals("user")) {
        final SecurityUser user = database.getUser();
        if (user != null) {
          identity = user.getIdentity(database);
        }
      } else if (identityType.equals("role")) {
        final Set<? extends SecurityRole> roles = database.getUser().getRoles();
        if (!roles.isEmpty()) {
          identity = roles.iterator().next().getIdentity(database);
        }
      } else {
        throw new ConfigurationException(
            "Wrong custom field '"
                + SecurityShared.ONCREATE_IDENTITY_TYPE
                + "' in class '"
                + cls.getName()
                + "' with value '"
                + identityType
                + "'. Supported ones are: 'user', 'role'");
      }

      if (identity != null && identity.getIdentity().isValid()) {
        for (String f : fields) {
          database.getSharedContext().getSecurity().allowIdentity(database, iDocument, f, identity);
        }
        return true;
      }
    }
    return false;
  }

  @SuppressWarnings("unchecked")
  public static boolean isAllowed(
      DatabaseSessionInternal database,
      final EntityImpl iDocument,
      final RestrictedOperation iAllowOperation,
      final boolean iReadOriginal) {
    final SchemaImmutableClass cls = DocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (cls != null && cls.isRestricted()) {

      if (database.getUser() == null) {
        return true;
      }

      if (database.getUser()
          .isRuleDefined(database, Rule.ResourceGeneric.BYPASS_RESTRICTED, null)) {
        if (database
            .getUser()
            .checkIfAllowed(database,
                Rule.ResourceGeneric.BYPASS_RESTRICTED, null, Role.PERMISSION_READ)
            != null)
        // BYPASS RECORD LEVEL SECURITY: ONLY "ADMIN" ROLE CAN BY DEFAULT
        {
          return true;
        }
      }

      final EntityImpl doc;
      if (iReadOriginal)
      // RELOAD TO AVOID HACKING OF "_ALLOW" FIELDS
      {
        try {
          doc = database.load(iDocument.getIdentity());
        } catch (RecordNotFoundException e) {
          return false;
        }

      } else {
        doc = iDocument;
      }

      return database
          .getMetadata()
          .getSecurity()
          .isAllowed(
              doc.field(RestrictedOperation.ALLOW_ALL.getFieldName()),
              doc.field(iAllowOperation.getFieldName()));
    }

    return true;
  }
}
