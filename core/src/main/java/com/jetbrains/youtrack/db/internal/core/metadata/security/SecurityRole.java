package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.metadata.security.Rule.ResourceGeneric;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;

/**
 * @since 03/11/14
 */
public interface SecurityRole extends Serializable {

  @Deprecated
  enum ALLOW_MODES {
    @Deprecated
    DENY_ALL_BUT,
    @Deprecated
    ALLOW_ALL_BUT
  }

  boolean allow(
      final Rule.ResourceGeneric resourceGeneric,
      String resourceSpecific,
      final int iCRUDOperation);

  boolean hasRule(final Rule.ResourceGeneric resourceGeneric, String resourceSpecific);

  SecurityRole addRule(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  SecurityRole grant(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  SecurityRole revoke(
      DatabaseSession session, final ResourceGeneric resourceGeneric, String resourceSpecific,
      final int iOperation);

  @Deprecated
  boolean allow(final String iResource, final int iCRUDOperation);

  @Deprecated
  boolean hasRule(final String iResource);

  @Deprecated
  SecurityRole addRule(DatabaseSession session, final String iResource, final int iOperation);

  @Deprecated
  SecurityRole grant(DatabaseSession session, final String iResource, final int iOperation);

  @Deprecated
  SecurityRole revoke(DatabaseSession session, final String iResource, final int iOperation);

  String getName(DatabaseSession session);

  SecurityRole.ALLOW_MODES getMode();

  SecurityRole setMode(final SecurityRole.ALLOW_MODES iMode);

  SecurityRole getParentRole();

  SecurityRole setParentRole(DatabaseSession session, final SecurityRole iParent);

  Set<Rule> getRuleSet();

  Identifiable getIdentity(DatabaseSession session);

  Map<String, SecurityPolicy> getPolicies(DatabaseSession session);

  SecurityPolicy getPolicy(DatabaseSession session, String resource);
}
