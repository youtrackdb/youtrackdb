package com.jetbrains.youtrack.db.internal.common.profiler;

import com.jetbrains.youtrack.db.internal.common.profiler.AbstractProfiler.ProfilerHookValue;
import com.jetbrains.youtrack.db.internal.common.types.OModifiableLong;

public class ModifiableLongProfileHookValue implements ProfilerHookValue {

  private final OModifiableLong value;

  public ModifiableLongProfileHookValue(OModifiableLong value) {
    this.value = value;
  }

  @Override
  public Object getValue() {
    return value.getValue();
  }
}
