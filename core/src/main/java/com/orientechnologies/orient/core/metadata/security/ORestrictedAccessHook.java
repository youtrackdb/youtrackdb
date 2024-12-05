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
package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTConfigurationException;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.metadata.schema.YTImmutableClass;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.Set;

/**
 * Checks the access against restricted resources. Restricted resources are those documents of
 * classes that implement ORestricted abstract class.
 */
public class ORestrictedAccessHook {

  public static boolean onRecordBeforeCreate(
      final YTDocument iDocument, YTDatabaseSessionInternal database) {
    final YTImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (cls != null && cls.isRestricted()) {
      String fieldNames = cls.getCustom(OSecurityShared.ONCREATE_FIELD);
      if (fieldNames == null) {
        fieldNames = ORestrictedOperation.ALLOW_ALL.getFieldName();
      }
      final String[] fields = fieldNames.split(",");
      String identityType = cls.getCustom(OSecurityShared.ONCREATE_IDENTITY_TYPE);
      if (identityType == null) {
        identityType = "user";
      }

      YTIdentifiable identity = null;
      if (identityType.equals("user")) {
        final YTSecurityUser user = database.getUser();
        if (user != null) {
          identity = user.getIdentity(database);
        }
      } else if (identityType.equals("role")) {
        final Set<? extends OSecurityRole> roles = database.getUser().getRoles();
        if (!roles.isEmpty()) {
          identity = roles.iterator().next().getIdentity(database);
        }
      } else {
        throw new YTConfigurationException(
            "Wrong custom field '"
                + OSecurityShared.ONCREATE_IDENTITY_TYPE
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
      YTDatabaseSessionInternal database,
      final YTDocument iDocument,
      final ORestrictedOperation iAllowOperation,
      final boolean iReadOriginal) {
    final YTImmutableClass cls = ODocumentInternal.getImmutableSchemaClass(database, iDocument);
    if (cls != null && cls.isRestricted()) {

      if (database.getUser() == null) {
        return true;
      }

      if (database.getUser()
          .isRuleDefined(database, ORule.ResourceGeneric.BYPASS_RESTRICTED, null)) {
        if (database
            .getUser()
            .checkIfAllowed(database,
                ORule.ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_READ)
            != null)
        // BYPASS RECORD LEVEL SECURITY: ONLY "ADMIN" ROLE CAN BY DEFAULT
        {
          return true;
        }
      }

      final YTDocument doc;
      if (iReadOriginal)
      // RELOAD TO AVOID HACKING OF "_ALLOW" FIELDS
      {
        try {
          doc = database.load(iDocument.getIdentity());
        } catch (YTRecordNotFoundException e) {
          return false;
        }

      } else {
        doc = iDocument;
      }

      return database
          .getMetadata()
          .getSecurity()
          .isAllowed(
              doc.field(ORestrictedOperation.ALLOW_ALL.getFieldName()),
              doc.field(iAllowOperation.getFieldName()));
    }

    return true;
  }
}
