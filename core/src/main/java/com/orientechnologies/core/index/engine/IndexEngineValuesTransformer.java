package com.orientechnologies.core.index.engine;

import com.orientechnologies.core.id.YTRID;
import java.util.Collection;

public interface IndexEngineValuesTransformer {

  Collection<YTRID> transformFromValue(Object value);
}
