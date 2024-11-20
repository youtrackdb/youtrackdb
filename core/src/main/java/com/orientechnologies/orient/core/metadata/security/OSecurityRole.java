package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 03/11/14
 */
public interface OSecurityRole extends Serializable {

  @Deprecated
  enum ALLOW_MODES {
    @Deprecated
    DENY_ALL_BUT,
    @Deprecated
    ALLOW_ALL_BUT
  }

  boolean allow(
      final ORule.ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iCRUDOperation);

  boolean hasRule(final ORule.ResourceGeneric resourceGeneric, String resourceSpecific);

  OSecurityRole addRule(
      final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation);

  OSecurityRole grant(
      final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation);

  OSecurityRole revoke(
      final ORule.ResourceGeneric resourceGeneric, String resourceSpecific, final int iOperation);

  @Deprecated
  boolean allow(final String iResource, final int iCRUDOperation);

  @Deprecated
  boolean hasRule(final String iResource);

  @Deprecated
  OSecurityRole addRule(final String iResource, final int iOperation);

  @Deprecated
  OSecurityRole grant(final String iResource, final int iOperation);

  @Deprecated
  OSecurityRole revoke(final String iResource, final int iOperation);

  String getName();

  OSecurityRole.ALLOW_MODES getMode();

  OSecurityRole setMode(final OSecurityRole.ALLOW_MODES iMode);

  OSecurityRole getParentRole();

  OSecurityRole setParentRole(final OSecurityRole iParent);

  Set<ORule> getRuleSet();

  OIdentifiable getIdentity();

  Map<String, OSecurityPolicy> getPolicies();

  OSecurityPolicy getPolicy(String resource);
}
