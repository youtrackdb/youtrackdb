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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule.ResourceGeneric;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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
public class Role extends Identity implements SecurityRole {

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
  protected Role parentRole;

  private final Map<Rule.ResourceGeneric, Rule> rules =
      new HashMap<Rule.ResourceGeneric, Rule>();

  /**
   * Constructor used in unmarshalling.
   */
  public Role() {
  }

  public Role(DatabaseSessionInternal db, final String iName, final Role iParent,
      final ALLOW_MODES iAllowMode) {
    this(db, iName, iParent, iAllowMode, null);
  }

  public Role(
      DatabaseSessionInternal db, final String iName,
      final Role iParent,
      final ALLOW_MODES iAllowMode,
      Map<String, SecurityPolicy> policies) {
    super(db, CLASS_NAME);
    getDocument(db).field("name", iName);

    parentRole = iParent;
    getDocument(db).field("inheritedRole",
        iParent != null ? iParent.getIdentity(db) : null);
    if (policies != null) {
      Map<String, Identifiable> p = new HashMap<>();
      policies.forEach((k, v) -> p.put(k,
          ((SecurityPolicyImpl) v).getElement(db)));
      getDocument(db).setProperty("policies", p);
    }

    updateRolesDocumentContent(db);
  }

  /**
   * Create the role by reading the source entity.
   */
  public Role(DatabaseSession session, final EntityImpl iSource) {
    fromStream((DatabaseSessionInternal) session, iSource);
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
  public void fromStream(DatabaseSessionInternal session, final EntityImpl iSource) {
    if (getDocument(session) != null) {
      return;
    }

    setDocument(session, iSource);

    var entity = getDocument(session);
    try {
      final Number modeField = entity.field("mode");
      if (modeField == null) {
        mode = ALLOW_MODES.DENY_ALL_BUT;
      } else if (modeField.byteValue() == STREAM_ALLOW) {
        mode = ALLOW_MODES.ALLOW_ALL_BUT;
      } else {
        mode = ALLOW_MODES.DENY_ALL_BUT;
      }

    } catch (Exception ex) {
      LogManager.instance().error(this, "illegal mode " + ex.getMessage(), ex);
      mode = ALLOW_MODES.DENY_ALL_BUT;
    }

    final Identifiable role = entity.field("inheritedRole");
    parentRole =
        role != null ? session.getMetadata().getSecurity().getRole(role) : null;

    boolean rolesNeedToBeUpdated = false;
    Object loadedRules = entity.field("rules");
    if (loadedRules instanceof Map) {
      loadOldVersionOfRules((Map<String, Number>) loadedRules);
    } else {
      final Set<EntityImpl> storedRules = (Set<EntityImpl>) loadedRules;
      if (storedRules != null) {
        for (EntityImpl ruleDoc : storedRules) {
          final Rule.ResourceGeneric resourceGeneric =
              Rule.ResourceGeneric.valueOf(ruleDoc.field("resourceGeneric"));
          if (resourceGeneric == null) {
            continue;
          }
          final Map<String, Byte> specificResources = ruleDoc.field("specificResources");
          final Byte access = ruleDoc.field("access");

          final Rule rule = new Rule(resourceGeneric, specificResources, access);
          rules.put(resourceGeneric, rule);
        }
      }

      // convert the format of roles presentation to classic one
      rolesNeedToBeUpdated = true;
    }

    if (getName(session).equals("admin") && !hasRule(Rule.ResourceGeneric.BYPASS_RESTRICTED, null))
    // FIX 1.5.1 TO ASSIGN database.bypassRestricted rule to the role
    {
      addRule(session, ResourceGeneric.BYPASS_RESTRICTED, null, Role.PERMISSION_ALL).save(session);
    }

    if (rolesNeedToBeUpdated) {
      updateRolesDocumentContent(session);
      save(session);
    }
  }

  public boolean allow(
      final Rule.ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iCRUDOperation) {
    final Rule rule = rules.get(resourceGeneric);
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

  public boolean hasRule(final Rule.ResourceGeneric resourceGeneric, String resourceSpecific) {
    Rule rule = rules.get(resourceGeneric);

    if (rule == null) {
      return false;
    }

    return resourceSpecific == null || rule.containsSpecificResource(resourceSpecific);
  }

  public Role addRule(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    Rule rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new Rule(resourceGeneric, null, null);
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
    final String specificResource = Rule.mapLegacyResourceToSpecificResource(iResource);
    final Rule.ResourceGeneric resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return allow(resourceGeneric, null, iCRUDOperation);
    }

    return allow(resourceGeneric, specificResource, iCRUDOperation);
  }

  @Deprecated
  @Override
  public boolean hasRule(String iResource) {
    final String specificResource = Rule.mapLegacyResourceToSpecificResource(iResource);
    final Rule.ResourceGeneric resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return hasRule(resourceGeneric, null);
    }

    return hasRule(resourceGeneric, specificResource);
  }

  @Deprecated
  @Override
  public SecurityRole addRule(DatabaseSession session, String iResource, int iOperation) {
    final String specificResource = Rule.mapLegacyResourceToSpecificResource(iResource);
    final Rule.ResourceGeneric resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return addRule(session, resourceGeneric, null, iOperation);
    }

    return addRule(session, resourceGeneric, specificResource, iOperation);
  }

  @Deprecated
  @Override
  public SecurityRole grant(DatabaseSession session, String iResource, int iOperation) {
    final String specificResource = Rule.mapLegacyResourceToSpecificResource(iResource);
    final Rule.ResourceGeneric resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return grant(session, resourceGeneric, null, iOperation);
    }

    return grant(session, resourceGeneric, specificResource, iOperation);
  }

  @Deprecated
  @Override
  public SecurityRole revoke(DatabaseSession session, String iResource, int iOperation) {
    final String specificResource = Rule.mapLegacyResourceToSpecificResource(iResource);
    final Rule.ResourceGeneric resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

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
  public Role grant(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    Rule rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new Rule(resourceGeneric, null, null);
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
  public Role revoke(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    if (iOperation == PERMISSION_NONE) {
      return this;
    }

    Rule rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new Rule(resourceGeneric, null, null);
      rules.put(resourceGeneric, rule);
    }

    rule.revokeAccess(resourceSpecific, iOperation);
    rules.put(resourceGeneric, rule);

    updateRolesDocumentContent(session);

    return this;
  }

  public String getName(DatabaseSession session) {
    return getDocument(session).field("name");
  }

  @Deprecated
  public ALLOW_MODES getMode() {
    return mode;
  }

  @Deprecated
  public Role setMode(final ALLOW_MODES iMode) {
    //    this.mode = iMode;
    //    entity.field("mode", mode == ALLOW_MODES.ALLOW_ALL_BUT ? STREAM_ALLOW : STREAM_DENY);
    return this;
  }

  public Role getParentRole() {
    return parentRole;
  }

  public Role setParentRole(DatabaseSession session, final SecurityRole iParent) {
    this.parentRole = (Role) iParent;
    getDocument(session).field("inheritedRole",
        parentRole != null ? parentRole.getIdentity(session) : null);
    return this;
  }

  @Override
  public Role save(DatabaseSessionInternal session) {
    getDocument(session).save();
    return this;
  }

  public Set<Rule> getRuleSet() {
    return new HashSet<Rule>(rules.values());
  }

  @Deprecated
  public Map<String, Byte> getRules() {
    final Map<String, Byte> result = new HashMap<String, Byte>();

    for (Rule rule : rules.values()) {
      String name = Rule.mapResourceGenericToLegacyResource(rule.getResourceGeneric());

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
    var database = DatabaseRecordThreadLocal.instance().getIfDefined();

    if (database != null) {
      return getName(database);
    }

    return "ORole";
  }

  @Override
  public Identifiable getIdentity(DatabaseSession session) {
    return getDocument(session);
  }

  private void loadOldVersionOfRules(final Map<String, Number> storedRules) {
    if (storedRules != null) {
      for (Entry<String, Number> a : storedRules.entrySet()) {
        Rule.ResourceGeneric resourceGeneric =
            Rule.mapLegacyResourceToGenericResource(a.getKey());
        Rule rule = rules.get(resourceGeneric);
        if (rule == null) {
          rule = new Rule(resourceGeneric, null, null);
          rules.put(resourceGeneric, rule);
        }

        String specificResource = Rule.mapLegacyResourceToSpecificResource(a.getKey());
        if (specificResource == null || specificResource.equals("*")) {
          rule.grantAccess(null, a.getValue().intValue());
        } else {
          rule.grantAccess(specificResource, a.getValue().intValue());
        }
      }
    }
  }

  private void updateRolesDocumentContent(DatabaseSession session) {
    getDocument(session).field("rules", getRules());
  }

  @Override
  public Map<String, SecurityPolicy> getPolicies(DatabaseSession db) {
    Map<String, Identifiable> policies = getDocument(db).getProperty("policies");
    if (policies == null) {
      return null;
    }
    Map<String, SecurityPolicy> result = new HashMap<>();
    policies.forEach(
        (key, value) -> {
          try {
            Entity rec = value.getRecord(db);
            result.put(key, new SecurityPolicyImpl(rec));
          } catch (RecordNotFoundException rnf) {
            // ignore
          }
        });
    return result;
  }

  @Override
  public SecurityPolicy getPolicy(DatabaseSession db, String resource) {
    Map<String, Identifiable> policies = getDocument(db).getProperty("policies");
    if (policies == null) {
      return null;
    }
    Identifiable entry = policies.get(resource);
    if (entry == null) {
      return null;
    }
    Entity policy;
    try {
      policy = entry.getRecord(db);

    } catch (RecordNotFoundException rnf) {
      return null;
    }

    return new SecurityPolicyImpl(policy);
  }
}
