package com.orientechnologies.orient.core.id;

public interface ChangeableIdentity {
  void addIdentityChangeListeners(IdentityChangeListener identityChangeListeners);

  void removeIdentityChangeListener(IdentityChangeListener identityChangeListener);

  boolean canChangeIdentity();
}
