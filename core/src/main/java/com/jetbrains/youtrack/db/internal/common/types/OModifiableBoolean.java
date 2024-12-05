package com.jetbrains.youtrack.db.internal.common.types;

/**
 * This internal API please do not use it.
 * <p>
 * <p>
 * href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 *
 * @since 19/12/14
 */
public class OModifiableBoolean {

  private boolean value;

  public OModifiableBoolean() {
    value = false;
  }

  public OModifiableBoolean(boolean value) {
    this.value = value;
  }

  public boolean getValue() {
    return value;
  }

  public void setValue(boolean value) {
    this.value = value;
  }
}
