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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.metadata.security.ORule.ResourceGeneric;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.Serial;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * Contains the user settings about security and permissions roles.<br> Allowed operation are the
 * classic CRUD, namely:
 *
 * <ul>
 *   <li>CREATE
 *   <li>READ
 *   <li>UPDATE
 *   <li>DELETE
 * </ul>
 * <p>
 * Mode = ALLOW (allow all but) or DENY (deny all but)
 */
@SuppressWarnings("unchecked")
public class ORole extends OIdentity implements OSecurityRole {

  public static final String ADMIN = "admin";
  public static final String CLASS_NAME = "ORole";
  public static final int PERMISSION_NONE = 0;
  public static final int PERMISSION_CREATE = registerPermissionBit(0, "Create");
  public static final int PERMISSION_READ = registerPermissionBit(1, "Read");
  public static final int PERMISSION_UPDATE = registerPermissionBit(2, "Update");
  public static final int PERMISSION_DELETE = registerPermissionBit(3, "Delete");
  public static final int PERMISSION_EXECUTE = registerPermissionBit(4, "Execute");
  public static final int PERMISSION_ALL =
      PERMISSION_CREATE
          + PERMISSION_READ
          + PERMISSION_UPDATE
          + PERMISSION_DELETE
          + PERMISSION_EXECUTE;
  protected static final byte STREAM_DENY = 0;
  protected static final byte STREAM_ALLOW = 1;
  @Serial
  private static final long serialVersionUID = 1L;
  // CRUD OPERATIONS
  private static Int2ObjectOpenHashMap<String> PERMISSION_BIT_NAMES;
  protected ALLOW_MODES mode = ALLOW_MODES.DENY_ALL_BUT;
  protected ORole parentRole;

  private final Map<ORule.ResourceGeneric, ORule> rules =
      new HashMap<ORule.ResourceGeneric, ORule>();

  /**
   * Constructor used in unmarshalling.
   */
  public ORole() {
  }

  public ORole(YTDatabaseSession session, final String iName, final ORole iParent,
      final ALLOW_MODES iAllowMode) {
    this(session, iName, iParent, iAllowMode, null);
  }

  public ORole(
      YTDatabaseSession session, final String iName,
      final ORole iParent,
      final ALLOW_MODES iAllowMode,
      Map<String, OSecurityPolicy> policies) {
    super(CLASS_NAME);
    getDocument(session).field("name", iName);

    parentRole = iParent;
    getDocument(session).field("inheritedRole",
        iParent != null ? iParent.getIdentity(session) : null);
    if (policies != null) {
      Map<String, YTIdentifiable> p = new HashMap<>();
      policies.forEach((k, v) -> p.put(k,
          ((OSecurityPolicyImpl) v).getElement((YTDatabaseSessionInternal) session)));
      getDocument(session).setProperty("policies", p);
    }

    updateRolesDocumentContent(session);
  }

  /**
   * Create the role by reading the source document.
   */
  public ORole(YTDatabaseSession session, final YTDocument iSource) {
    fromStream((YTDatabaseSessionInternal) session, iSource);
  }

  /**
   * Convert the permission code to a readable string.
   *
   * @param iPermission Permission to convert
   * @return String representation of the permission
   */
  public static String permissionToString(final int iPermission) {
    int permission = iPermission;
    final StringBuilder returnValue = new StringBuilder(128);
    for (Entry<Integer, String> p : PERMISSION_BIT_NAMES.entrySet()) {
      if ((permission & p.getKey()) == p.getKey()) {
        if (returnValue.length() > 0) {
          returnValue.append(", ");
        }
        returnValue.append(p.getValue());
        permission &= ~p.getKey();
      }
    }
    if (permission != 0) {
      if (returnValue.length() > 0) {
        returnValue.append(", ");
      }
      returnValue.append("Unknown 0x");
      returnValue.append(Integer.toHexString(permission));
    }

    return returnValue.toString();
  }

  public static int registerPermissionBit(final int bitNo, final String iName) {
    if (bitNo < 0 || bitNo > 31) {
      throw new IndexOutOfBoundsException(
          "Permission bit number must be positive and less than 32");
    }

    final int value = 1 << bitNo;
    if (PERMISSION_BIT_NAMES == null) {
      PERMISSION_BIT_NAMES = new Int2ObjectOpenHashMap<>();
    }

    if (PERMISSION_BIT_NAMES.containsKey(value)) {
      throw new IndexOutOfBoundsException("Permission bit number " + bitNo + " already in use");
    }

    PERMISSION_BIT_NAMES.put(value, iName);
    return value;
  }

  @Override
  public void fromStream(YTDatabaseSessionInternal session, final YTDocument iSource) {
    if (getDocument(session) != null) {
      return;
    }

    setDocument(session, iSource);

    var document = getDocument(session);
    try {
      final Number modeField = document.field("mode");
      if (modeField == null) {
        mode = ALLOW_MODES.DENY_ALL_BUT;
      } else if (modeField.byteValue() == STREAM_ALLOW) {
        mode = ALLOW_MODES.ALLOW_ALL_BUT;
      } else {
        mode = ALLOW_MODES.DENY_ALL_BUT;
      }

    } catch (Exception ex) {
      OLogManager.instance().error(this, "illegal mode " + ex.getMessage(), ex);
      mode = ALLOW_MODES.DENY_ALL_BUT;
    }

    final YTIdentifiable role = document.field("inheritedRole");
    parentRole =
        role != null ? session.getMetadata().getSecurity().getRole(role) : null;

    boolean rolesNeedToBeUpdated = false;
    Object loadedRules = document.field("rules");
    if (loadedRules instanceof Map) {
      loadOldVersionOfRules((Map<String, Number>) loadedRules);
    } else {
      final Set<YTDocument> storedRules = (Set<YTDocument>) loadedRules;
      if (storedRules != null) {
        for (YTDocument ruleDoc : storedRules) {
          final ORule.ResourceGeneric resourceGeneric =
              ORule.ResourceGeneric.valueOf(ruleDoc.field("resourceGeneric"));
          if (resourceGeneric == null) {
            continue;
          }
          final Map<String, Byte> specificResources = ruleDoc.field("specificResources");
          final Byte access = ruleDoc.field("access");

          final ORule rule = new ORule(resourceGeneric, specificResources, access);
          rules.put(resourceGeneric, rule);
        }
      }

      // convert the format of roles presentation to classic one
      rolesNeedToBeUpdated = true;
    }

    if (getName(session).equals("admin") && !hasRule(ORule.ResourceGeneric.BYPASS_RESTRICTED, null))
    // FIX 1.5.1 TO ASSIGN database.bypassRestricted rule to the role
    {
      addRule(session, ResourceGeneric.BYPASS_RESTRICTED, null, ORole.PERMISSION_ALL).save(session);
    }

    if (rolesNeedToBeUpdated) {
      updateRolesDocumentContent(session);
      save(session);
    }
  }

  public boolean allow(
      final ORule.ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iCRUDOperation) {
    final ORule rule = rules.get(resourceGeneric);
    if (rule != null) {
      final Boolean allowed = rule.isAllowed(resourceSpecific, iCRUDOperation);
      if (allowed != null) {
        return allowed;
      }
    }

    if (parentRole != null)
    // DELEGATE TO THE PARENT ROLE IF ANY
    {
      return parentRole.allow(resourceGeneric, resourceSpecific, iCRUDOperation);
    }

    return false;
  }

  public boolean hasRule(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific) {
    ORule rule = rules.get(resourceGeneric);

    if (rule == null) {
      return false;
    }

    return resourceSpecific == null || rule.containsSpecificResource(resourceSpecific);
  }

  public ORole addRule(
      YTDatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    ORule rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new ORule(resourceGeneric, null, null);
      rules.put(resourceGeneric, rule);
    }

    rule.grantAccess(resourceSpecific, iOperation);

    rules.put(resourceGeneric, rule);

    updateRolesDocumentContent(session);

    return this;
  }

  @Deprecated
  @Override
  public boolean allow(String iResource, int iCRUDOperation) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return allow(resourceGeneric, null, iCRUDOperation);
    }

    return allow(resourceGeneric, specificResource, iCRUDOperation);
  }

  @Deprecated
  @Override
  public boolean hasRule(String iResource) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return hasRule(resourceGeneric, null);
    }

    return hasRule(resourceGeneric, specificResource);
  }

  @Deprecated
  @Override
  public OSecurityRole addRule(YTDatabaseSession session, String iResource, int iOperation) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return addRule(session, resourceGeneric, null, iOperation);
    }

    return addRule(session, resourceGeneric, specificResource, iOperation);
  }

  @Deprecated
  @Override
  public OSecurityRole grant(YTDatabaseSession session, String iResource, int iOperation) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return grant(session, resourceGeneric, null, iOperation);
    }

    return grant(session, resourceGeneric, specificResource, iOperation);
  }

  @Deprecated
  @Override
  public OSecurityRole revoke(YTDatabaseSession session, String iResource, int iOperation) {
    final String specificResource = ORule.mapLegacyResourceToSpecificResource(iResource);
    final ORule.ResourceGeneric resourceGeneric =
        ORule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return revoke(session, resourceGeneric, null, iOperation);
    }

    return revoke(session, resourceGeneric, specificResource, iOperation);
  }

  /**
   * Grant a permission to the resource.
   *
   * @return
   */
  public ORole grant(
      YTDatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    ORule rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new ORule(resourceGeneric, null, null);
      rules.put(resourceGeneric, rule);
    }

    rule.grantAccess(resourceSpecific, iOperation);

    rules.put(resourceGeneric, rule);
    updateRolesDocumentContent(session);
    return this;
  }

  /**
   * Revoke a permission to the resource.
   */
  public ORole revoke(
      YTDatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    if (iOperation == PERMISSION_NONE) {
      return this;
    }

    ORule rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new ORule(resourceGeneric, null, null);
      rules.put(resourceGeneric, rule);
    }

    rule.revokeAccess(resourceSpecific, iOperation);
    rules.put(resourceGeneric, rule);

    updateRolesDocumentContent(session);

    return this;
  }

  public String getName(YTDatabaseSession session) {
    return getDocument(session).field("name");
  }

  @Deprecated
  public ALLOW_MODES getMode() {
    return mode;
  }

  @Deprecated
  public ORole setMode(final ALLOW_MODES iMode) {
    //    this.mode = iMode;
    //    document.field("mode", mode == ALLOW_MODES.ALLOW_ALL_BUT ? STREAM_ALLOW : STREAM_DENY);
    return this;
  }

  public ORole getParentRole() {
    return parentRole;
  }

  public ORole setParentRole(YTDatabaseSession session, final OSecurityRole iParent) {
    this.parentRole = (ORole) iParent;
    getDocument(session).field("inheritedRole",
        parentRole != null ? parentRole.getIdentity(session) : null);
    return this;
  }

  @Override
  public ORole save(YTDatabaseSessionInternal session) {
    getDocument(session).save(ORole.class.getSimpleName());
    return this;
  }

  public Set<ORule> getRuleSet() {
    return new HashSet<ORule>(rules.values());
  }

  @Deprecated
  public Map<String, Byte> getRules() {
    final Map<String, Byte> result = new HashMap<String, Byte>();

    for (ORule rule : rules.values()) {
      String name = ORule.mapResourceGenericToLegacyResource(rule.getResourceGeneric());

      if (rule.getAccess() != null) {
        result.put(name, rule.getAccess());
      }

      for (Map.Entry<String, Byte> specificResource : rule.getSpecificResources().entrySet()) {
        result.put(name + "." + specificResource.getKey(), specificResource.getValue());
      }
    }

    return result;
  }

  @Override
  public String toString() {
    var database = ODatabaseRecordThreadLocal.instance().getIfDefined();

    if (database != null) {
      return getName(database);
    }

    return "ORole";
  }

  @Override
  public YTIdentifiable getIdentity(YTDatabaseSession session) {
    return getDocument(session);
  }

  private void loadOldVersionOfRules(final Map<String, Number> storedRules) {
    if (storedRules != null) {
      for (Entry<String, Number> a : storedRules.entrySet()) {
        ORule.ResourceGeneric resourceGeneric =
            ORule.mapLegacyResourceToGenericResource(a.getKey());
        ORule rule = rules.get(resourceGeneric);
        if (rule == null) {
          rule = new ORule(resourceGeneric, null, null);
          rules.put(resourceGeneric, rule);
        }

        String specificResource = ORule.mapLegacyResourceToSpecificResource(a.getKey());
        if (specificResource == null || specificResource.equals("*")) {
          rule.grantAccess(null, a.getValue().intValue());
        } else {
          rule.grantAccess(specificResource, a.getValue().intValue());
        }
      }
    }
  }

  private void updateRolesDocumentContent(YTDatabaseSession session) {
    getDocument(session).field("rules", getRules());
  }

  @Override
  public Map<String, OSecurityPolicy> getPolicies(YTDatabaseSession session) {
    Map<String, YTIdentifiable> policies = getDocument(session).getProperty("policies");
    if (policies == null) {
      return null;
    }
    Map<String, OSecurityPolicy> result = new HashMap<>();
    policies.forEach(
        (key, value) -> {
          try {
            YTEntity rec = value.getRecord();
            result.put(key, new OSecurityPolicyImpl(rec));
          } catch (YTRecordNotFoundException rnf) {
            // ignore
          }
        });
    return result;
  }

  @Override
  public OSecurityPolicy getPolicy(YTDatabaseSession session, String resource) {
    Map<String, YTIdentifiable> policies = getDocument(session).getProperty("policies");
    if (policies == null) {
      return null;
    }
    YTIdentifiable entry = policies.get(resource);
    if (entry == null) {
      return null;
    }
    YTEntity policy;
    try {
      policy = entry.getRecord();

    } catch (YTRecordNotFoundException rnf) {
      return null;
    }

    return new OSecurityPolicyImpl(policy);
  }
}
