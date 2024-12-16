package com.jetbrains.youtrack.db.internal.core.id;

public interface ChangeableIdentity {

  void addIdentityChangeListener(IdentityChangeListener identityChangeListeners);

  void removeIdentityChangeListener(IdentityChangeListener identityChangeListener);

  boolean canChangeIdentity();
}
