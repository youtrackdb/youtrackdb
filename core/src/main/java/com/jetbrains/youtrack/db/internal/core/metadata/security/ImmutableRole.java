package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule.ResourceGeneric;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @since 03/11/14
 */
public class ImmutableRole implements SecurityRole {

  private static final long serialVersionUID = 1L;
  private final ALLOW_MODES mode;
  private final SecurityRole parentRole;

  private final Map<Rule.ResourceGeneric, Rule> rules =
      new HashMap<Rule.ResourceGeneric, Rule>();
  private final String name;
  private final RID rid;
  private final Map<String, SecurityPolicy> policies;

  public ImmutableRole(DatabaseSessionInternal session, SecurityRole role) {
    if (role.getParentRole() == null) {
      this.parentRole = null;
    } else {
      this.parentRole = new ImmutableRole(session, role.getParentRole());
    }

    this.mode = role.getMode();
    this.name = role.getName(session);
    this.rid = role.getIdentity(session).getIdentity();

    for (Rule rule : role.getRuleSet()) {
      rules.put(rule.getResourceGeneric(), rule);
    }
    Map<String, SecurityPolicy> policies = role.getPolicies(session);
    if (policies != null) {
      Map<String, SecurityPolicy> result = new HashMap<String, SecurityPolicy>();
      policies
          .entrySet()
          .forEach(
              x -> result.put(x.getKey(), new ImmutableSecurityPolicy(session, x.getValue())));
      this.policies = result;
    } else {
      this.policies = null;
    }
  }

  public ImmutableRole(
      ImmutableRole parent,
      String name,
      Map<ResourceGeneric, Rule> rules,
      Map<String, ImmutableSecurityPolicy> policies) {
    this.parentRole = parent;

    this.mode = ALLOW_MODES.DENY_ALL_BUT;
    this.name = name;
    this.rid = new RecordId(-1, -1);
    this.rules.putAll(rules);
    this.policies = (Map<String, SecurityPolicy>) (Map) policies;
  }

  public boolean allow(
      final Rule.ResourceGeneric resourceGeneric,
      final String resourceSpecific,
      final int iCRUDOperation) {
    Rule rule = rules.get(resourceGeneric);
    if (rule == null) {
      rule = rules.get(Rule.ResourceGeneric.ALL);
    }

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

  public SecurityRole addRule(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    throw new UnsupportedOperationException();
  }

  public SecurityRole grant(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    throw new UnsupportedOperationException();
  }

  public Role revoke(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation) {
    throw new UnsupportedOperationException();
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

  @Override
  public SecurityRole addRule(DatabaseSession session, String iResource, int iOperation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SecurityRole grant(DatabaseSession session, String iResource, int iOperation) {
    throw new UnsupportedOperationException();
  }

  @Override
  public SecurityRole revoke(DatabaseSession session, String iResource, int iOperation) {
    throw new UnsupportedOperationException();
  }

  public String getName(DatabaseSession session) {
    return name;
  }

  public ALLOW_MODES getMode() {
    return mode;
  }

  public Role setMode(final ALLOW_MODES iMode) {
    throw new UnsupportedOperationException();
  }

  public SecurityRole getParentRole() {
    return parentRole;
  }

  public Role setParentRole(DatabaseSession session, final SecurityRole iParent) {
    throw new UnsupportedOperationException();
  }

  public Set<Rule> getRuleSet() {
    return new HashSet<Rule>(rules.values());
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public Identifiable getIdentity(DatabaseSession session) {
    return rid;
  }

  @Override
  public Map<String, SecurityPolicy> getPolicies(DatabaseSession session) {
    return policies;
  }

  @Override
  public SecurityPolicy getPolicy(DatabaseSession session, String resource) {
    if (policies == null) {
      return null;
    }
    return policies.get(resource);
  }
}
