package com.orientechnologies.core.index.multivalue;

import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.index.engine.IndexEngineValuesTransformer;
import java.util.Collection;

public final class MultiValuesTransformer implements IndexEngineValuesTransformer {

  public static final MultiValuesTransformer INSTANCE = new MultiValuesTransformer();

  @Override
  public Collection<YTRID> transformFromValue(Object value) {
    //noinspection unchecked
    return (Collection<YTRID>) value;
  }
}
