package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.security.ORule.ResourceGeneric;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
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
      ODatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  OSecurityRole grant(
      ODatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  OSecurityRole revoke(
      ODatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  @Deprecated
  boolean allow(final String iResource, final int iCRUDOperation);

  @Deprecated
  boolean hasRule(final String iResource);

  @Deprecated
  OSecurityRole addRule(ODatabaseSession session, final String iResource, final int iOperation);

  @Deprecated
  OSecurityRole grant(ODatabaseSession session, final String iResource, final int iOperation);

  @Deprecated
  OSecurityRole revoke(ODatabaseSession session, final String iResource, final int iOperation);

  String getName(ODatabaseSession session);

  OSecurityRole.ALLOW_MODES getMode();

  OSecurityRole setMode(final OSecurityRole.ALLOW_MODES iMode);

  OSecurityRole getParentRole();

  OSecurityRole setParentRole(ODatabaseSession session, final OSecurityRole iParent);

  Set<ORule> getRuleSet();

  OIdentifiable getIdentity(ODatabaseSession session);

  Map<String, OSecurityPolicy> getPolicies(ODatabaseSession session);

  OSecurityPolicy getPolicy(ODatabaseSession session, String resource);
}
