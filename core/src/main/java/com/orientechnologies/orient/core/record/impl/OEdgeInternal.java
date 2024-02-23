package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;

public interface OEdgeInternal extends OEdge, OElementInternal {

  @Nullable
  ODocument getBaseDocument();

  @Override
  default Set<String> getPropertyNames() {
    return filterPropertyNames(getPropertyNamesWithoutFiltration());
  }

  static Set<String> filterPropertyNames(Set<String> propertyNames) {
    var propertiesToRemove = new ArrayList<String>();

    for (var propertyName : propertyNames) {
      if (propertyName.equals(DIRECTION_IN) || propertyName.equals(DIRECTION_OUT)) {
        propertiesToRemove.add(propertyName);
      }
    }

    if (propertiesToRemove.isEmpty()) {
      return propertyNames;
    }

    for (var propertyToRemove : propertiesToRemove) {
      propertyNames.remove(propertyToRemove);
    }

    return propertyNames;
  }

  @Override
  default <RET> RET getProperty(String name) {
    checkPropertyName(name);

    return getPropertyWithoutValidation(name);
  }

  static void checkPropertyName(String name) {
    if (name.equals(DIRECTION_OUT) || name.equals(DIRECTION_IN)) {
      throw new IllegalArgumentException(
          "Property name " + name + " is booked as a name that can be used to manage edges.");
    }
  }

  @Override
  default boolean exists() {
    var baseDocument = getBaseDocument();
    if (baseDocument == null) {
      return false;
    }

    return baseDocument.exists();
  }

  @Override
  default void setProperty(String name, Object value) {
    checkPropertyName(name);

    setPropertyWithoutValidation(name, value);
  }

  @Override
  default boolean hasProperty(final String propertyName) {
    checkPropertyName(propertyName);
    var baseDocument = getBaseDocument();
    if (baseDocument == null) {
      return false;
    }

    return baseDocument.hasProperty(propertyName);
  }

  @Override
  default void setProperty(String name, Object value, OType... fieldType) {
    checkPropertyName(name);
    setPropertyWithoutValidation(name, value, fieldType);
  }

  @Override
  default <RET> RET removeProperty(String name) {
    checkPropertyName(name);

    return removePropertyWithoutValidation(name);
  }

  @Nullable
  @Override
  default OIdentifiable getLinkProperty(String name) {
    checkPropertyName(name);

    var baseDocument = getBaseDocument();
    if (baseDocument == null) {
      return null;
    }

    return baseDocument.getLinkProperty(name);
  }

  @Override
  default Set<String> getPropertyNamesWithoutFiltration() {
    var baseDocument = getBaseDocument();
    if (baseDocument == null) {
      return Collections.emptySet();
    }
    return baseDocument.getPropertyNamesWithoutFiltration();
  }

  @Override
  default <RET> RET getPropertyWithoutValidation(String name) {
    var baseDocument = getBaseDocument();
    if (baseDocument == null) {
      return null;
    }

    return baseDocument.getPropertyWithoutValidation(name);
  }

  @Nullable
  @Override
  default OIdentifiable getLinkPropertyWithoutValidation(String name) {
    var baseDocument = getBaseDocument();
    if (baseDocument == null) {
      return null;
    }

    return baseDocument.getLinkPropertyWithoutValidation(name);
  }

  @Override
  default void setPropertyWithoutValidation(String name, Object value) {
    var baseDocument = getBaseDocument();
    if (baseDocument == null) {
      promoteToRegularEdge();
      baseDocument = getBaseDocument();

      if (baseDocument == null) {
        throw new UnsupportedOperationException("This edge is not backed by a document.");
      }
    }

    baseDocument.setPropertyWithoutValidation(name, value);
  }

  @Override
  default void setPropertyWithoutValidation(String name, Object value, OType... type) {
    var baseDocument = getBaseDocument();
    if (baseDocument == null) {
      promoteToRegularEdge();

      baseDocument = getBaseDocument();
      if (baseDocument == null) {
        throw new UnsupportedOperationException("This edge is not backed by a document.");
      }
    }

    baseDocument.setPropertyWithoutValidation(name, value, type);
  }

  @Override
  default <RET> RET removePropertyWithoutValidation(String name) {
    var baseDocument = getBaseDocument();
    if (baseDocument == null) {
      throw new UnsupportedOperationException("This edge is not backed by a document.");
    }

    return baseDocument.removePropertyWithoutValidation(name);
  }

  void promoteToRegularEdge();
}
