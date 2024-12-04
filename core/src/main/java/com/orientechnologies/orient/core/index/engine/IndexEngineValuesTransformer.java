package com.orientechnologies.orient.core.index.engine;

import com.orientechnologies.orient.core.id.YTRID;
import java.util.Collection;

public interface IndexEngineValuesTransformer {

  Collection<YTRID> transformFromValue(Object value);
}
