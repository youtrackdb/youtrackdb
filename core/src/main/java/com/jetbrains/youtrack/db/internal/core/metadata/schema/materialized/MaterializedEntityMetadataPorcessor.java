package com.jetbrains.youtrack.db.internal.core.metadata.schema.materialized;

import com.jetbrains.youtrack.db.api.exception.SchemaException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nonnull;

public abstract class MaterializedEntityMetadataPorcessor {

  @Nonnull
  public static MaterializedEntityMetadata validateAndFetchMaterializedEntityAccessMethods(
      @Nonnull Class<? extends MaterializedEntity> materializedEntityInterface) {
    return doValidateAndFetchMaterializedEntityAccessMethods(materializedEntityInterface,
        new HashMap<>());
  }

  private static MaterializedEntityMetadata doValidateAndFetchMaterializedEntityAccessMethods(
      @Nonnull Class<? extends MaterializedEntity> materializedEntityInterface,
      @Nonnull HashMap<Class<? extends MaterializedEntity>, MaterializedEntityMetadata> processedEntities) {
    if (processedEntities.containsKey(materializedEntityInterface)) {
      return processedEntities.get(materializedEntityInterface);
    }

    if (materializedEntityInterface.isLocalClass()) {
      throw new SchemaException("Materialized entity class must not be a local class");
    }
    if (materializedEntityInterface.isAnonymousClass()) {
      throw new SchemaException("Materialized entity class must not be an anonymous class");
    }

    //prevent infinite recursion in entity dependency graph
    //real value will be set at the end of method execution
    var metadata = new MaterializedEntityMetadata(materializedEntityInterface,
        new ArrayList<>(),
        new ArrayList<>(),
        new HashMap<>());
    processedEntities.put(materializedEntityInterface, metadata);

    if (!materializedEntityInterface.isInterface()) {
      throw new SchemaException("Materialized entity class must be an interface");
    }

    var requiredEntities = new HashSet<Class<? extends MaterializedEntity>>();
    var parentInterfaces = materializedEntityInterface.getInterfaces();
    var parentEntities = new ArrayList<Class<? extends MaterializedEntity>>();

    for (var parentInterface : parentInterfaces) {
      if (parentInterface == MaterializedEntity.class) {
        continue;
      }

      if (MaterializedEntity.class.isAssignableFrom(parentInterface)) {
        @SuppressWarnings("unchecked")
        var parentEntity = (Class<? extends MaterializedEntity>) parentInterface;

        parentEntities.add(parentEntity);
        requiredEntities.add(parentEntity);
      }
    }

    var properties = new ArrayList<MaterializedPropertyMetadata>();
    var methods = materializedEntityInterface.getDeclaredMethods();
    validateMethodsAndFetchGettersAndSetters(methods, properties, requiredEntities);
    requiredEntities.add(materializedEntityInterface);

    var requiredDeclarations = new HashMap<Class<? extends MaterializedEntity>, MaterializedEntityMetadata>();
    for (var requiredEntity : requiredEntities) {
      var requiredDeclaration = doValidateAndFetchMaterializedEntityAccessMethods(requiredEntity,
          processedEntities);
      requiredDeclarations.put(requiredEntity, requiredDeclaration);
    }

    metadata.properties().addAll(properties);
    metadata.parents().addAll(parentEntities);
    metadata.requiredDeclarations().putAll(requiredDeclarations);

    return metadata;
  }

  private static void validateMethodsAndFetchGettersAndSetters(Method[] methods,
      List<MaterializedPropertyMetadata> properties,
      HashSet<Class<? extends MaterializedEntity>> requiredEntities) {

    HashMap<String, Method> getters = new HashMap<>();
    HashMap<String, Method> setters = new HashMap<>();

    for (var method : methods) {
      if (method.getDeclaringClass().equals(MaterializedEntity.class)) {
        continue;
      }

      var methodName = method.getName();
      if (methodName.startsWith("get") || methodName.startsWith("is")) {
        var firstLetterIndex = methodName.startsWith("get") ? 3 : 2;
        var propertyName = fetchPropertyNameFromMethodName(methodName, firstLetterIndex);
        getters.put(propertyName, method);
      } else if (methodName.startsWith("set")) {
        setters.put(fetchPropertyNameFromMethodName(methodName, 3), method);
      } else {
        if (!method.isDefault()) {
          throw new SchemaException("Method " + methodName + " is not a getter or setter. "
              + "All methods except getters and setters must be implemented in materialized entity");
        }
        if (method.isSynthetic() || method.isBridge()) {
          throw new SchemaException("Method " + methodName + " is a synthetic or bridge method. "
              + "Such methods are not allowed in materialized entity.");
        }
      }
    }

    for (var getter : getters.values()) {
      validateGetter(getter, requiredEntities);
    }

    for (var entry : setters.entrySet()) {
      var setter = entry.getValue();
      validateSetter(entry.getValue(), requiredEntities);

      var relatedGetter = getters.get(entry.getKey());
      var getterReturnType = relatedGetter.getReturnType();
      if (getterReturnType != setter.getParameterTypes()[0]) {
        throw new SchemaException("Getter and setter for property " + entry.getKey()
            + " must have the same type");
      }
      if (getterReturnType.equals(List.class) || getterReturnType.equals(Set.class)
          || getterReturnType.equals(Map.class)) {
        var getterGenericReturnType = relatedGetter.getGenericReturnType();
        var setterParameterType = setter.getGenericParameterTypes()[0];

        if (!getterGenericReturnType.equals(setterParameterType)) {
          throw new SchemaException("Getter and setter for property " + entry.getKey()
              + " must have the same generic type");
        }
      }
    }

    for (var entry : getters.entrySet()) {
      var propertyName = entry.getKey();
      var getter = entry.getValue();
      var propertyType = getter.getReturnType();

      Class<?> linkedType = null;
      if (propertyType.equals(List.class) || propertyType.equals(Set.class)) {
        linkedType = (Class<?>) ((ParameterizedType) getter.getGenericReturnType()).getActualTypeArguments()[0];
      } else if (propertyType.equals(Map.class)) {
        linkedType = (Class<?>) ((ParameterizedType) getter.getGenericReturnType()).getActualTypeArguments()[1];
      }

      properties.add(
          new MaterializedPropertyMetadata(propertyName, propertyType, linkedType, getter,
              setters.get(propertyName)));
    }
  }

  private static String fetchPropertyNameFromMethodName(String methodName, int firstLetterIndex) {
    var firstLetter = Character.toLowerCase(methodName.charAt(firstLetterIndex));

    String propertyName;
    if (methodName.length() == firstLetterIndex + 1) {
      propertyName = String.valueOf(firstLetter);
    } else {
      propertyName = firstLetter + methodName.substring(firstLetterIndex + 1);
    }
    return propertyName;
  }


  private static void validateSetter(@Nonnull Method method,
      @Nonnull HashSet<Class<? extends MaterializedEntity>> requiredEntities) {
    var methodName = method.getName();
    if (!(methodName.length() > 3
        && method.getParameterCount() == 1
        && method.getReturnType().equals(void.class) && Character.isUpperCase(
        methodName.charAt(3)))) {
      throw new SchemaException(
          "Method " + methodName + " is not a setter. Setter method name must start with 'set', "
              + "has one argument and return void. "
              + "'set' prefix has to be followed by a property name that starts with upper case.");
    }

    var parameterType = method.getParameterTypes()[0];

    validateNonCollectionType(parameterType, methodName, false, requiredEntities);
  }

  private static void validateGetter(@Nonnull Method method,
      @Nonnull HashSet<Class<? extends MaterializedEntity>> requiredEntities) {
    var methodName = method.getName();

    if (method.getParameterCount() == 0) {
      if (methodName.startsWith("get")) {
        if (!(methodName.length() > 3 && Character.isUpperCase(
            methodName.charAt(3)))) {
          throw new SchemaException(invalidGetterNameMessage(methodName));
        }
      } else if (!(methodName.length() > 2 && Character.isUpperCase(
          methodName.charAt(2)) && method.getReturnType().equals(boolean.class))) {
        throw new SchemaException(invalidGetterNameMessage(methodName));
      }
    } else {
      throw new SchemaException(invalidGetterNameMessage(methodName));
    }

    var genericType = method.getGenericReturnType();
    validateGenericType(genericType, methodName, requiredEntities);
  }

  private static String invalidGetterNameMessage(@Nonnull String methodName) {
    return "Method " + methodName
        + " is not a getter. Getter method name must start with 'get' or 'is' "
        + "and return a value. 'get' prefix has to be followed by a property name that starts with upper case. "
        + "'is' prefix is allowed only for boolean properties.";
  }

  private static void validateGenericType(@Nonnull Type genericType, @Nonnull String methodName,
      @Nonnull HashSet<Class<? extends MaterializedEntity>> requiredEntities) {
    if (genericType instanceof Class<?> rawClass) {
      validateNonCollectionType(rawClass, methodName, true, requiredEntities);
      return;
    }

    if (!(genericType instanceof ParameterizedType parameterizedType)) {
      throw new SchemaException(invalidGetterMethodReturnTypeMessage(methodName));
    }

    var rawType = parameterizedType.getRawType();
    if (!(rawType instanceof Class<?> ownerType)) {
      throw new SchemaException(invalidGetterMethodReturnTypeMessage(methodName));
    }

    if (ownerType.equals(List.class) || ownerType.equals(Set.class)) {
      var actualTypeArguments = parameterizedType.getActualTypeArguments();
      if (actualTypeArguments.length != 1) {
        throw new SchemaException(invalidGetterMethodReturnTypeMessage(methodName));
      }

      validateGenericType(actualTypeArguments[0], methodName, requiredEntities);
    } else if (ownerType.equals(Map.class)) {
      var actualTypeArguments = parameterizedType.getActualTypeArguments();
      if (actualTypeArguments.length != 2) {
        throw new SchemaException(invalidGetterMethodReturnTypeMessage(methodName));
      }
      var keyType = actualTypeArguments[0];
      var valueType = actualTypeArguments[1];

      if (!(keyType.equals(String.class))) {
        throw new SchemaException(invalidGetterMethodReturnTypeMessage(methodName));
      }

      validateGenericType(valueType, methodName, requiredEntities);
    }
  }

  private static void validateNonCollectionType(@Nonnull Class<?> rawClass,
      @Nonnull String methodName,
      boolean getter, @Nonnull HashSet<Class<? extends MaterializedEntity>> requiredEntities) {
    if (!(rawClass.isPrimitive() || rawClass.equals(String.class) || rawClass.equals(Byte.class)
        || rawClass.equals(Short.class) || rawClass.equals(Integer.class) || rawClass.equals(
        Long.class) || rawClass.equals(Float.class) || rawClass.equals(Double.class) || rawClass
        .equals(Boolean.class) || MaterializedEntity.class.isAssignableFrom(rawClass))) {
      if (getter) {
        throw new SchemaException(invalidGetterMethodReturnTypeMessage(methodName));
      } else {
        throw new SchemaException(invalidSetterMethodArgumentMessage(methodName));
      }
    }

    if (MaterializedEntity.class.isAssignableFrom(rawClass)) {
      @SuppressWarnings("unchecked")
      var materializedEntity = (Class<? extends MaterializedEntity>) rawClass;
      requiredEntities.add(materializedEntity);
    }
  }

  private static String invalidGetterMethodReturnTypeMessage(@Nonnull String methodName) {
    return "Getter method " + methodName +
        " must return a primitive type, collection interface or materialized entity."
        + " Following collection interfaces are supported: "
        + " String, List, Set, Map."
        + " Collection interfaces must have a generic type argument. "
        + " Generic type argument must be one of listed above. Map must have String as a key type."
        + " Wrapper classes are also supported.";
  }

  private static String invalidSetterMethodArgumentMessage(@Nonnull String methodName) {
    return "Setter method " + methodName +
        " must accept primitive type, String class or materialized entity as an argument."
        + " Wrapper classes are also supported.";
  }
}
