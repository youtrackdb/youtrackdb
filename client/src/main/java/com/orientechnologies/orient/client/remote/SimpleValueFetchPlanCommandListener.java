package com.orientechnologies.orient.client.remote;

import com.jetbrains.youtrack.db.internal.core.command.CommandResultListener;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;

public interface SimpleValueFetchPlanCommandListener extends CommandResultListener {

  void linkdedBySimpleValue(EntityImpl doc);
}
