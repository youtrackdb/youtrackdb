package com.orientechnologies.orient.client.remote;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;

public interface SimpleValueFetchPlanCommandListener extends OCommandResultListener {

  void linkdedBySimpleValue(YTEntityImpl doc);
}
