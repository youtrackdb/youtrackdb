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
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule.ResourceGeneric;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.type.IdentityWrapper;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.Nonnull;

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
public class Role extends IdentityWrapper implements SecurityRole {

  public static final String POLICIES = "policies";
  public static final String INHERITED_ROLE = "inheritedRole";
  public static final String NAME = "name";
  public static final String RULES = "rules";

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


  // CRUD OPERATIONS
  private static Int2ObjectOpenHashMap<String> PERMISSION_BIT_NAMES;

  private final ConcurrentHashMap<ResourceGeneric, Rule> rules = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, SecurityPolicy> policies = new ConcurrentHashMap<>();

  private final String name;
  private volatile SecurityRole parent;

  public Role(DatabaseSessionInternal db, final String iName, final Role parent) {
    this(db, iName, parent, null);
  }

  public Role(
      DatabaseSessionInternal db, final String name, final Role parent,
      Map<String, SecurityPolicy> policies) {
    super(db, CLASS_NAME);

    this.name = name;
    this.parent = parent;

    if (policies != null) {
      this.policies.putAll(policies);
    }

    save(db);
  }

  /**
   * Create the role by reading the source entity.
   */
  public Role(DatabaseSessionInternal session, final EntityImpl source) {
    super(source);

    name = source.getProperty(NAME);
    var inheritedParent = source.getLinkProperty(INHERITED_ROLE);

    if (inheritedParent != null) {
      parent = session.getMetadata().getSecurity().getRole(inheritedParent);
    } else {
      parent = null;
    }

    Map<String, Number> storedRules = source.getProperty(RULES);
    if (storedRules != null) {
      for (var a : storedRules.entrySet()) {
        var resourceGeneric =
            Rule.mapLegacyResourceToGenericResource(a.getKey());

        var rule = rules.get(resourceGeneric);
        if (rule == null) {
          rule = new Rule(resourceGeneric, null, null);
          rules.put(resourceGeneric, rule);
        }

        var specificResource = Rule.mapLegacyResourceToSpecificResource(a.getKey());
        if (specificResource == null || specificResource.equals("*")) {
          rule.grantAccess(null, a.getValue().intValue());
        } else {
          rule.grantAccess(specificResource, a.getValue().intValue());
        }
      }
    }

    Map<String, Identifiable> policies = source.getProperty(POLICIES);
    if (policies != null) {
      policies.forEach(
          (key, value) -> {
            try {
              Entity rec = value.getRecord(session);
              this.policies.put(key, new SecurityPolicyImpl(rec));
            } catch (RecordNotFoundException rnf) {
              // ignore
            }
          });
    }
  }

  @Override
  protected void toEntity(@Nonnull DatabaseSessionInternal db, @Nonnull EntityImpl entity) {
    entity.setProperty(NAME, name);
    entity.setProperty(INHERITED_ROLE, parent != null ? parent.getIdentity() : null);

    var storedRules = entity.getOrCreateEmbeddedMap(RULES);
    storedRules.clear();
    storedRules.putAll(getEncodedRules());

    var storedPolicies = new HashMap<String, Identifiable>();
    for (var policy : policies.entrySet()) {
      storedPolicies.put(policy.getKey(), policy.getValue().getIdentity());
    }

    var entityPolicies = entity.getOrCreateLinkMap(POLICIES);
    entityPolicies.clear();
    entityPolicies.putAll(storedPolicies);
  }

  /**
   * Convert the permission code to a readable string.
   *
   * @param iPermission Permission to convert
   * @return String representation of the permission
   */
  public static String permissionToString(final int iPermission) {
    var permission = iPermission;
    final var returnValue = new StringBuilder(128);
    for (var p : PERMISSION_BIT_NAMES.entrySet()) {
      if ((permission & p.getKey()) == p.getKey()) {
        if (!returnValue.isEmpty()) {
          returnValue.append(", ");
        }
        returnValue.append(p.getValue());
        permission &= ~p.getKey();
      }
    }
    if (permission != 0) {
      if (!returnValue.isEmpty()) {
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

    final var value = 1 << bitNo;
    if (PERMISSION_BIT_NAMES == null) {
      PERMISSION_BIT_NAMES = new Int2ObjectOpenHashMap<>();
    }

    if (PERMISSION_BIT_NAMES.containsKey(value)) {
      throw new IndexOutOfBoundsException("Permission bit number " + bitNo + " already in use");
    }

    PERMISSION_BIT_NAMES.put(value, iName);
    return value;
  }

  static void generateSchema(DatabaseSessionInternal session) {
    SchemaProperty p;
    var schema = session.getMetadata().getSchema();
    final var roleClass = schema.getClassInternal(CLASS_NAME);

    final var rules = roleClass.getProperty(session, RULES);
    if (rules == null) {
      roleClass.createProperty(session, RULES, PropertyType.EMBEDDEDSET);
    }

    if (!roleClass.existsProperty(session, INHERITED_ROLE)) {
      roleClass.createProperty(session, INHERITED_ROLE, PropertyType.LINK, roleClass);
    }

    p = roleClass.getProperty(session, NAME);
    if (p == null) {
      p = roleClass.createProperty(session, NAME, PropertyType.STRING).
          setMandatory(session, true)
          .setNotNull(session, true);
    }

    if (roleClass.getInvolvedIndexes(session, NAME) == null) {
      p.createIndex(session, INDEX_TYPE.UNIQUE);
    }

    if (!roleClass.existsProperty(session, POLICIES)) {
      roleClass.createProperty(session, POLICIES, PropertyType.LINKMAP,
          schema.getClass(SecurityPolicy.CLASS_NAME));
    }
  }

  private Map<ResourceGeneric, Rule> getRules() {
    return Collections.unmodifiableMap(rules);
  }

  public SecurityRole getParentRole() {
    return parent;
  }

  public boolean allow(
      final Rule.ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iCRUDOperation) {
    final var rule = getRules().get(resourceGeneric);
    if (rule != null) {
      final var allowed = rule.isAllowed(resourceSpecific, iCRUDOperation);
      if (allowed != null) {
        return allowed;
      }
    }

    if (parent != null) // DELEGATE TO THE PARENT ROLE IF ANY
    {
      return parent.allow(resourceGeneric, resourceSpecific, iCRUDOperation);
    }

    return false;
  }

  public boolean hasRule(final Rule.ResourceGeneric resourceGeneric, String resourceSpecific) {
    var rules = getRules();
    var rule = rules.get(resourceGeneric);

    if (rule == null) {
      return false;
    }

    return resourceSpecific == null || rule.containsSpecificResource(resourceSpecific);
  }

  public Role addRule(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    var rule = rules.get(resourceGeneric);
    if (rule == null) {
      rule = new Rule(resourceGeneric, null, null);
      rules.put(resourceGeneric, rule);
    }

    rule.grantAccess(resourceSpecific, iOperation);

    rules.put(resourceGeneric, rule);
    save((DatabaseSessionInternal) session);
    return this;
  }

  @Deprecated
  @Override
  public boolean allow(String iResource, int iCRUDOperation) {
    final var specificResource = Rule.mapLegacyResourceToSpecificResource(iResource);
    final var resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return allow(resourceGeneric, null, iCRUDOperation);
    }

    return allow(resourceGeneric, specificResource, iCRUDOperation);
  }

  @Deprecated
  @Override
  public boolean hasRule(String iResource) {
    final var specificResource = Rule.mapLegacyResourceToSpecificResource(iResource);
    final var resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return hasRule(resourceGeneric, null);
    }

    return hasRule(resourceGeneric, specificResource);
  }

  @Deprecated
  @Override
  public SecurityRole addRule(DatabaseSession session, String iResource, int iOperation) {
    final var specificResource = Rule.mapLegacyResourceToSpecificResource(iResource);
    final var resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return addRule(session, resourceGeneric, null, iOperation);
    }

    return addRule(session, resourceGeneric, specificResource, iOperation);
  }

  @Deprecated
  @Override
  public SecurityRole grant(DatabaseSession session, String iResource, int iOperation) {
    final var specificResource = Rule.mapLegacyResourceToSpecificResource(iResource);
    final var resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return grant(session, resourceGeneric, null, iOperation);
    }

    return grant(session, resourceGeneric, specificResource, iOperation);
  }

  @Deprecated
  @Override
  public SecurityRole revoke(DatabaseSession session, String iResource, int iOperation) {
    final var specificResource = Rule.mapLegacyResourceToSpecificResource(iResource);
    final var resourceGeneric =
        Rule.mapLegacyResourceToGenericResource(iResource);

    if (specificResource == null || specificResource.equals("*")) {
      return revoke(session, resourceGeneric, null, iOperation);
    }

    return revoke(session, resourceGeneric, specificResource, iOperation);
  }

  /**
   * Grant a permission to the resource.
   */
  public Role grant(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    var rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new Rule(resourceGeneric, null, null);
      rules.put(resourceGeneric, rule);
    }

    rule.grantAccess(resourceSpecific, iOperation);

    rules.put(resourceGeneric, rule);
    save((DatabaseSessionInternal) session);
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

    var rule = rules.get(resourceGeneric);

    if (rule == null) {
      rule = new Rule(resourceGeneric, null, null);
      rules.put(resourceGeneric, rule);
    }

    rule.revokeAccess(resourceSpecific, iOperation);
    rules.put(resourceGeneric, rule);

    save((DatabaseSessionInternal) session);

    return this;
  }

  public String getName(DatabaseSession session) {
    return name;
  }

  public void setParentRole(DatabaseSession session, final SecurityRole parent) {
    this.parent = parent;
  }

  public Set<Rule> getRuleSet() {
    return new HashSet<>(getRules().values());
  }

  public Map<String, Byte> getEncodedRules() {
    final Map<String, Byte> result = new HashMap<>();

    var rules = getRules();
    for (var rule : rules.values()) {
      var name = Rule.mapResourceGenericToLegacyResource(rule.getResourceGeneric());

      if (rule.getAccess() != null) {
        result.put(name, rule.getAccess());
      }

      for (var specificResource : rule.getSpecificResources().entrySet()) {
        result.put(name + "." + specificResource.getKey(), specificResource.getValue());
      }
    }

    return result;
  }

  @Override
  public Map<String, SecurityPolicy> getPolicies(DatabaseSession db) {
    return policies;
  }

  @Override
  public SecurityPolicy getPolicy(DatabaseSession db, String resource) {
    return policies.get(resource);
  }
}

