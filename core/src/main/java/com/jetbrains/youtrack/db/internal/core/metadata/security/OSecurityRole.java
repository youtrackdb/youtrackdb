package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.metadata.security.ORule.ResourceGeneric;
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
      YTDatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  OSecurityRole grant(
      YTDatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  OSecurityRole revoke(
      YTDatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  @Deprecated
  boolean allow(final String iResource, final int iCRUDOperation);

  @Deprecated
  boolean hasRule(final String iResource);

  @Deprecated
  OSecurityRole addRule(YTDatabaseSession session, final String iResource, final int iOperation);

  @Deprecated
  OSecurityRole grant(YTDatabaseSession session, final String iResource, final int iOperation);

  @Deprecated
  OSecurityRole revoke(YTDatabaseSession session, final String iResource, final int iOperation);

  String getName(YTDatabaseSession session);

  OSecurityRole.ALLOW_MODES getMode();

  OSecurityRole setMode(final OSecurityRole.ALLOW_MODES iMode);

  OSecurityRole getParentRole();

  OSecurityRole setParentRole(YTDatabaseSession session, final OSecurityRole iParent);

  Set<ORule> getRuleSet();

  YTIdentifiable getIdentity(YTDatabaseSession session);

  Map<String, OSecurityPolicy> getPolicies(YTDatabaseSession session);

  OSecurityPolicy getPolicy(YTDatabaseSession session, String resource);
}
