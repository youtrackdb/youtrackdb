package com.orientechnologies.orient.core.id;

public interface ChangeableIdentity {
  void addIdentityChangeListener(IdentityChangeListener identityChangeListeners);

  void removeIdentityChangeListener(IdentityChangeListener identityChangeListener);

  boolean canChangeIdentity();
}
