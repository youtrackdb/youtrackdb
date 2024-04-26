package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ORecordLazyMultiValue;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.OEmptyRecordId;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.collections4.IterableUtils;

public interface OVertexInternal extends OVertex, OElementInternal {
  @Nonnull
  ODocument getBaseDocument();

  @Override
  default Set<String> getPropertyNames() {
    return filterPropertyNames(getBaseDocument().getPropertyNamesWithoutFiltration());
  }

  @Override
  default Set<String> getPropertyNamesWithoutFiltration() {
    return getBaseDocument().getPropertyNamesWithoutFiltration();
  }

  static Set<String> filterPropertyNames(Set<String> propertyNames) {
    var propertiesToRemove = new ArrayList<String>();

    for (var propertyName : propertyNames) {
      if (propertyName.startsWith(DIRECTION_IN_PREFIX)
          || propertyName.startsWith(DIRECTION_OUT_PREFIX)) {
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

    return getBaseDocument().getPropertyWithoutValidation(name);
  }

  @Override
  default <RET> RET getPropertyWithoutValidation(String name) {
    return getBaseDocument().getPropertyWithoutValidation(name);
  }

  @Override
  default <RET> RET getPropertyOnLoadValue(String name) {
    return getBaseDocument().getPropertyOnLoadValue(name);
  }

  @Nullable
  @Override
  default OIdentifiable getLinkPropertyWithoutValidation(String name) {
    return getBaseDocument().getLinkPropertyWithoutValidation(name);
  }

  @Nullable
  @Override
  default OIdentifiable getLinkProperty(String name) {
    checkPropertyName(name);

    return getBaseDocument().getLinkProperty(name);
  }

  static void checkPropertyName(String name) {
    if (name.startsWith(DIRECTION_OUT_PREFIX) || name.startsWith(DIRECTION_IN_PREFIX)) {
      throw new IllegalArgumentException(
          "Property name " + name + " is booked as a name that can be used to manage edges.");
    }
  }

  @Override
  default void setProperty(String name, Object value) {
    checkPropertyName(name);

    getBaseDocument().setPropertyWithoutValidation(name, value);
  }

  @Override
  default void setPropertyWithoutValidation(String name, Object value) {
    getBaseDocument().setPropertyWithoutValidation(name, value);
  }

  @Override
  default boolean hasProperty(final String propertyName) {
    checkPropertyName(propertyName);

    return getBaseDocument().hasProperty(propertyName);
  }

  @Override
  default void setProperty(String name, Object value, OType... fieldType) {
    checkPropertyName(name);

    getBaseDocument().setPropertyWithoutValidation(name, value, fieldType);
  }

  @Override
  default void setPropertyWithoutValidation(String name, Object value, OType... type) {
    getBaseDocument().setPropertyWithoutValidation(name, value, type);
  }

  @Override
  default <RET> RET removeProperty(String name) {
    checkPropertyName(name);

    return getBaseDocument().removePropertyWithoutValidation(name);
  }

  @Override
  default <RET> RET removePropertyWithoutValidation(String name) {
    return getBaseDocument().removePropertyWithoutValidation(name);
  }

  @Override
  default Iterable<OVertex> getVertices(ODirection direction) {
    return getVertices(direction, (String[]) null);
  }

  @Override
  default Set<String> getEdgeNames() {
    return getEdgeNames(ODirection.BOTH);
  }

  @Override
  default Set<String> getEdgeNames(ODirection direction) {
    var propertyNames = getBaseDocument().getPropertyNamesWithoutFiltration();
    var edgeNames = new HashSet<String>();

    for (var propertyName : propertyNames) {
      if (isConnectionToEdge(direction, propertyName)) {
        edgeNames.add(propertyName);
      }
    }

    return edgeNames;
  }

  static boolean isConnectionToEdge(ODirection direction, String propertyName) {
    if (propertyName.endsWith(DIRECT_LINK_SUFFIX)) {
      return false;
    }

    return switch (direction) {
      case OUT -> propertyName.startsWith(DIRECTION_OUT_PREFIX);
      case IN -> propertyName.startsWith(DIRECTION_IN_PREFIX);
      case BOTH ->
          propertyName.startsWith(DIRECTION_OUT_PREFIX)
              || propertyName.startsWith(DIRECTION_IN_PREFIX);
    };
  }

  @Override
  default Iterable<OVertex> getVertices(ODirection direction, String... type) {
    if (direction == ODirection.BOTH) {
      return IterableUtils.chainedIterable(
          getVertices(ODirection.OUT, type), getVertices(ODirection.IN, type));
    } else {
      Iterable<OEdge> edges = getEdgesInternal(direction, type, true);
      return new OEdgeToVertexIterable(edges, direction);
    }
  }

  @Override
  default Iterable<OVertex> getVertices(ODirection direction, OClass... type) {
    List<String> types = new ArrayList<>();
    if (type != null) {
      for (OClass t : type) {
        types.add(t.getName());
      }
    }

    return getVertices(direction, types.toArray(new String[] {}));
  }

  @Override
  default OEdge addEdge(OVertex to) {
    return addEdge(to, OEdgeInternal.CLASS_NAME);
  }

  @Override
  default OEdge addEdge(OVertex to, String type) {
    ODatabaseDocument db = getDatabase();
    return db.newEdge(this, to, type == null ? OEdgeInternal.CLASS_NAME : type);
  }

  @Override
  default OEdge addEdge(OVertex to, OClass type) {
    final String className;
    if (type != null) {
      className = type.getName();
    } else {
      className = OEdgeInternal.CLASS_NAME;
    }

    return addEdge(to, className);
  }

  @Override
  default Iterable<OEdge> getEdges(ODirection direction, OClass... type) {
    List<String> types = new ArrayList<>();
    if (type != null) {
      for (OClass t : type) {
        types.add(t.getName());
      }
    }
    return getEdges(direction, types.toArray(new String[] {}));
  }

  @Override
  default boolean isUnloaded() {
    return getBaseDocument().isUnloaded();
  }

  @Override
  default Iterable<OEdge> getEdges(ODirection direction) {
    var prefixes =
        switch (direction) {
          case IN -> new String[] {DIRECTION_IN_PREFIX};
          case OUT -> new String[] {DIRECTION_OUT_PREFIX};
          case BOTH -> new String[] {DIRECTION_IN_PREFIX, DIRECTION_OUT_PREFIX};
        };

    Set<String> candidateClasses = new HashSet<>();

    var doc = getBaseDocument();
    for (var prefix : prefixes) {
      for (String fieldName : doc.calculatePropertyNames()) {
        if (fieldName.startsWith(prefix) && !fieldName.endsWith(DIRECT_LINK_SUFFIX)) {
          if (fieldName.equals(prefix)) {
            candidateClasses.add(OEdgeInternal.CLASS_NAME);
          } else {
            candidateClasses.add(fieldName.substring(prefix.length()));
          }
        }
      }
    }

    return getEdges(direction, candidateClasses.toArray(new String[] {}));
  }

  @Override
  default boolean exists() {
    return getBaseDocument().exists();
  }

  @Override
  default Iterable<OEdge> getEdges(ODirection direction, String... labels) {
    return getEdgesInternal(direction, labels, false);
  }

  private Iterable<OEdge> getEdgesInternal(
      ODirection direction, String[] labels, boolean useDirectLinks) {
    var db = (ODatabaseDocumentInternal) getDatabase();
    var schema = db.getMetadata().getImmutableSchemaSnapshot();

    labels = resolveAliases(schema, labels);
    Collection<String> fieldNames = null;
    var doc = getBaseDocument();
    if (labels != null && labels.length > 0) {
      // EDGE LABELS: CREATE FIELD NAME TABLE (FASTER THAN EXTRACT FIELD NAMES FROM THE DOCUMENT)
      var toLoadFieldNames = getEdgeFieldNames(schema, direction, labels);

      if (toLoadFieldNames != null) {
        // EARLY FETCH ALL THE FIELDS THAT MATTERS
        doc.deserializeFields(toLoadFieldNames.toArray(new String[] {}));
        fieldNames = toLoadFieldNames;
      }
    }

    if (fieldNames == null) {
      fieldNames = doc.calculatePropertyNames();
    }

    var iterables = new ArrayList<Iterable<OEdge>>(fieldNames.size());
    for (var fieldName : fieldNames) {
      if (fieldName.endsWith(DIRECT_LINK_SUFFIX)) {
        continue;
      }

      final OPair<ODirection, String> connection =
          getConnection(schema, direction, fieldName, labels);
      if (connection == null)
      // SKIP THIS FIELD
      {
        continue;
      }

      Object fieldValue;

      if (useDirectLinks) {
        var directConnectionFieldName = getDirectEdgeLinkFieldName(fieldName);
        var directConnectionLink = doc.getPropertyWithoutValidation(directConnectionFieldName);

        if (directConnectionLink != null) {
          fieldValue = doc.getPropertyWithoutValidation(directConnectionFieldName);
        } else {
          fieldValue = doc.getPropertyWithoutValidation(fieldName);
        }
      } else {
        fieldValue = doc.getPropertyWithoutValidation(fieldName);
      }

      if (fieldValue != null) {
        if (fieldValue instanceof OIdentifiable) {
          var coll = Collections.singleton(fieldValue);
          iterables.add(new OEdgeIterator(this, coll, coll.iterator(), connection, labels, 1));
        } else if (fieldValue instanceof Collection<?> coll) {
          // CREATE LAZY Iterable AGAINST COLLECTION FIELD
          if (coll instanceof ORecordLazyMultiValue) {
            iterables.add(
                new OEdgeIterator(
                    this,
                    coll,
                    ((ORecordLazyMultiValue) coll).rawIterator(),
                    connection,
                    labels,
                    coll.size()));
          } else {
            iterables.add(new OEdgeIterator(this, coll, coll.iterator(), connection, labels, -1));
          }

        } else if (fieldValue instanceof ORidBag) {
          iterables.add(
              new OEdgeIterator(
                  this,
                  fieldValue,
                  ((ORidBag) fieldValue).rawIterator(),
                  connection,
                  labels,
                  ((ORidBag) fieldValue).size()));
        }
      }
    }

    if (iterables.size() == 1) {
      return iterables.get(0);
    } else if (iterables.isEmpty()) {
      return Collections.emptyList();
    }

    //noinspection unchecked
    return IterableUtils.chainedIterable(iterables.toArray(new Iterable[0]));
  }

  private static ArrayList<String> getEdgeFieldNames(
      OSchema schema, final ODirection iDirection, String... classNames) {
    if (classNames == null)
    // FALL BACK TO LOAD ALL FIELD NAMES
    {
      return null;
    }

    if (classNames.length == 1 && classNames[0].equalsIgnoreCase(OEdgeInternal.CLASS_NAME))
    // DEFAULT CLASS, TREAT IT AS NO CLASS/LABEL
    {
      return null;
    }

    Set<String> allClassNames = new HashSet<>();
    for (String className : classNames) {
      allClassNames.add(className);
      OClass clazz = schema.getClass(className);
      if (clazz != null) {
        allClassNames.add(clazz.getName()); // needed for aliases
        Collection<OClass> subClasses = clazz.getAllSubclasses();
        for (OClass subClass : subClasses) {
          allClassNames.add(subClass.getName());
        }
      }
    }

    var result = new ArrayList<String>(2 * allClassNames.size());
    for (String className : allClassNames) {
      switch (iDirection) {
        case OUT:
          result.add(DIRECTION_OUT_PREFIX + className);
          break;
        case IN:
          result.add(DIRECTION_IN_PREFIX + className);
          break;
        case BOTH:
          result.add(DIRECTION_OUT_PREFIX + className);
          result.add(DIRECTION_IN_PREFIX + className);
          break;
      }
    }

    return result;
  }

  static OPair<ODirection, String> getConnection(
      final OSchema schema,
      final ODirection direction,
      final String fieldName,
      String... classNames) {
    if (classNames != null
        && classNames.length == 1
        && classNames[0].equalsIgnoreCase(OEdgeInternal.CLASS_NAME))
    // DEFAULT CLASS, TREAT IT AS NO CLASS/LABEL
    {
      classNames = null;
    }

    if (direction == ODirection.OUT || direction == ODirection.BOTH) {
      // FIELDS THAT STARTS WITH "out_"
      if (fieldName.startsWith(DIRECTION_OUT_PREFIX)) {
        if (classNames == null || classNames.length == 0) {
          return new OPair<>(ODirection.OUT, getConnectionClass(ODirection.OUT, fieldName));
        }

        // CHECK AGAINST ALL THE CLASS NAMES
        for (String clsName : classNames) {
          if (fieldName.equals(DIRECTION_OUT_PREFIX + clsName)) {
            return new OPair<>(ODirection.OUT, clsName);
          }

          // GO DOWN THROUGH THE INHERITANCE TREE
          OClass type = schema.getClass(clsName);
          if (type != null) {
            for (OClass subType : type.getAllSubclasses()) {
              clsName = subType.getName();

              if (fieldName.equals(DIRECTION_OUT_PREFIX + clsName)) {
                return new OPair<>(ODirection.OUT, clsName);
              }
            }
          }
        }
      }
    }

    if (direction == ODirection.IN || direction == ODirection.BOTH) {
      // FIELDS THAT STARTS WITH "in_"
      if (fieldName.startsWith(DIRECTION_IN_PREFIX)) {
        if (classNames == null || classNames.length == 0) {
          return new OPair<>(ODirection.IN, getConnectionClass(ODirection.IN, fieldName));
        }

        // CHECK AGAINST ALL THE CLASS NAMES
        for (String clsName : classNames) {

          if (fieldName.equals(DIRECTION_IN_PREFIX + clsName)) {
            return new OPair<>(ODirection.IN, clsName);
          }

          // GO DOWN THROUGH THE INHERITANCE TREE
          OClass type = schema.getClass(clsName);
          if (type != null) {
            for (OClass subType : type.getAllSubclasses()) {
              clsName = subType.getName();
              if (fieldName.equals(DIRECTION_IN_PREFIX + clsName)) {
                return new OPair<>(ODirection.IN, clsName);
              }
            }
          }
        }
      }
    }

    // NOT FOUND
    return null;
  }

  private static void replaceLinks(
      final ODocument vertex,
      final String fieldName,
      final OIdentifiable iVertexToRemove,
      final OIdentifiable newVertex) {
    if (vertex == null) {
      return;
    }

    final Object fieldValue =
        iVertexToRemove != null
            ? vertex.getPropertyWithoutValidation(fieldName)
            : vertex.removePropertyWithoutValidation(fieldName);
    if (fieldValue == null) {
      return;
    }

    if (fieldValue instanceof OIdentifiable) {
      // SINGLE RECORD

      if (iVertexToRemove != null) {
        if (!fieldValue.equals(iVertexToRemove)) {
          return;
        }
        vertex.setPropertyWithoutValidation(fieldName, newVertex);
      }

    } else if (fieldValue instanceof ORidBag bag) {
      // COLLECTION OF RECORDS: REMOVE THE ENTRY
      boolean found = false;
      final Iterator<OIdentifiable> it = bag.rawIterator();
      while (it.hasNext()) {
        if (it.next().equals(iVertexToRemove)) {
          // REMOVE THE OLD ENTRY
          found = true;
          it.remove();
        }
      }
      if (found)
      // ADD THE NEW ONE
      {
        bag.add(newVertex);
      }

    } else if (fieldValue instanceof Collection) {
      @SuppressWarnings("unchecked")
      final Collection<OIdentifiable> col = (Collection<OIdentifiable>) fieldValue;

      if (col.remove(iVertexToRemove)) {
        col.add(newVertex);
      }
    }

    vertex.save();
  }

  static void deleteLinks(OVertex delegate) {
    Iterable<OEdge> allEdges = delegate.getEdges(ODirection.BOTH);
    List<OEdge> items = new ArrayList<>();
    for (OEdge edge : allEdges) {
      items.add(edge);
    }
    for (OEdge edge : items) {
      edge.delete();
    }
  }

  @Override
  default ORID moveTo(final String className, final String clusterName) {
    var db = getDatabase();
    if (checkDeletedInTx(getIdentity())) {
      throw new ORecordNotFoundException(
          getIdentity(), "The vertex " + getIdentity() + " has been deleted");
    }
    boolean moveTx = !db.getTransaction().isActive();
    try {
      if (moveTx) {
        db.begin();
      }

      final ORID oldIdentity = getIdentity().copy();

      final ORecord oldRecord = oldIdentity.getRecord();
      if (oldRecord == null) {
        throw new ORecordNotFoundException(
            oldIdentity, "The vertex " + oldIdentity + " has been deleted");
      }

      final ODocument doc = getBaseDocument().copy();

      // DELETE THE OLD RECORD FIRST TO AVOID ISSUES WITH UNIQUE CONSTRAINTS
      copyRidBags(oldRecord, doc);
      detachRidbags(oldRecord);
      db.delete(oldRecord);

      var delegate = new OVertexDelegate(doc);
      final Iterable<OEdge> outEdges = delegate.getEdges(ODirection.OUT);
      final Iterable<OEdge> inEdges = delegate.getEdges(ODirection.IN);
      if (className != null) {
        doc.setClassName(className);
      }

      // SAVE THE NEW VERTEX
      doc.setDirty();
      // RESET IDENTITY
      ORecordInternal.setIdentity(doc, new OEmptyRecordId());
      db.save(doc, clusterName);
      final ORID newIdentity = doc.getIdentity();

      // CONVERT OUT EDGES
      for (OEdge oe : outEdges) {
        final OIdentifiable inVLink = oe.getVertexLink(ODirection.IN);
        var optSchemaType = oe.getSchemaType();

        String schemaType;
        //noinspection OptionalIsPresent
        if (optSchemaType.isPresent()) {
          schemaType = optSchemaType.get().getName();
        } else {
          schemaType = null;
        }

        final String inFieldName = getEdgeLinkFieldName(ODirection.IN, schemaType, true);

        // link to itself
        ODocument inRecord;
        if (inVLink.equals(oldIdentity)) {
          inRecord = doc;
        } else {
          inRecord = inVLink.getRecord();
        }
        //noinspection deprecation
        if (oe.isLightweight()) {
          // REPLACE ALL REFS IN inVertex
          replaceLinks(inRecord, inFieldName, oldIdentity, newIdentity);
        } else {
          // REPLACE WITH NEW VERTEX
          ((OElementInternal) oe)
              .setPropertyWithoutValidation(OEdgeInternal.DIRECTION_OUT, newIdentity);

          var directInFieldName = getDirectEdgeLinkFieldName(inFieldName);
          if (inRecord.hasProperty(directInFieldName)) {
            replaceLinks(inRecord, directInFieldName, oldIdentity, newIdentity);
          }
        }

        db.save(oe);
      }

      for (OEdge ine : inEdges) {
        final OIdentifiable outVLink = ine.getVertexLink(ODirection.OUT);

        var optSchemaType = ine.getSchemaType();

        String schemaType;
        //noinspection OptionalIsPresent
        if (optSchemaType.isPresent()) {
          schemaType = optSchemaType.get().getName();
        } else {
          schemaType = null;
        }

        final String outFieldName = getEdgeLinkFieldName(ODirection.OUT, schemaType, true);

        ODocument outRecord;
        if (outVLink.equals(oldIdentity)) {
          outRecord = doc;
        } else {
          outRecord = outVLink.getRecord();
        }
        //noinspection deprecation
        if (ine.isLightweight()) {
          // REPLACE ALL REFS IN outVertex
          replaceLinks(outRecord, outFieldName, oldIdentity, newIdentity);
        } else {
          // REPLACE WITH NEW VERTEX
          ((OEdgeInternal) ine).setPropertyWithoutValidation(OEdge.DIRECTION_IN, newIdentity);
          var directOutFieldName = getDirectEdgeLinkFieldName(outFieldName);

          if (outRecord.hasProperty(directOutFieldName)) {
            replaceLinks(outRecord, directOutFieldName, oldIdentity, newIdentity);
          }
        }
        db.save(ine);
      }

      // FINAL SAVE
      db.save(doc);
      if (moveTx) {
        db.commit();
      }
      return newIdentity;
    } catch (RuntimeException ex) {
      if (moveTx) {
        db.rollback();
      }
      throw ex;
    }
  }

  private static void detachRidbags(ORecord oldRecord) {
    ODocument oldDoc = (ODocument) oldRecord;
    for (String field : oldDoc.getPropertyNamesWithoutFiltration()) {
      if (field.equalsIgnoreCase(OEdgeInternal.DIRECTION_OUT)
          || field.equalsIgnoreCase(OEdgeInternal.DIRECTION_IN)
          || field.startsWith(DIRECTION_OUT_PREFIX)
          || field.startsWith(DIRECTION_IN_PREFIX)
          || field.startsWith("OUT_")
          || field.startsWith("IN_")) {
        Object val = oldDoc.rawField(field);
        if (val instanceof ORidBag) {
          oldDoc.removePropertyWithoutValidation(field);
        }
      }
    }
  }

  static boolean checkDeletedInTx(ORID id) {
    ODatabaseDocument db = ODatabaseRecordThreadLocal.instance().get();
    if (db == null) {
      return false;
    }

    final ORecordOperation oper = db.getTransaction().getRecordEntry(id);
    if (oper == null) {
      return id.isTemporary();
    } else {
      return oper.type == ORecordOperation.DELETED;
    }
  }

  private static void copyRidBags(ORecord oldRecord, ODocument newDoc) {
    ODocument oldDoc = (ODocument) oldRecord;
    for (String field : oldDoc.getPropertyNamesWithoutFiltration()) {
      if (field.equalsIgnoreCase(OEdgeInternal.DIRECTION_OUT)
          || field.equalsIgnoreCase(OEdgeInternal.DIRECTION_IN)
          || field.startsWith(DIRECTION_OUT_PREFIX)
          || field.startsWith(DIRECTION_IN_PREFIX)
          || field.startsWith("OUT_")
          || field.startsWith("IN_")) {
        Object val = oldDoc.rawField(field);
        if (val instanceof ORidBag bag) {
          if (!bag.isEmbedded()) {
            ORidBag newBag = new ORidBag();
            Iterator<OIdentifiable> rawIter = bag.rawIterator();
            while (rawIter.hasNext()) {
              newBag.add(rawIter.next());
            }
            newDoc.setPropertyWithoutValidation(field, newBag);
          }
        }
      }
    }
  }

  private static String getConnectionClass(final ODirection iDirection, final String iFieldName) {
    if (iDirection == ODirection.OUT) {
      if (iFieldName.length() > DIRECTION_OUT_PREFIX.length()) {
        return iFieldName.substring(DIRECTION_OUT_PREFIX.length());
      }
    } else if (iDirection == ODirection.IN) {
      if (iFieldName.length() > DIRECTION_IN_PREFIX.length()) {
        return iFieldName.substring(DIRECTION_IN_PREFIX.length());
      }
    }
    return OEdgeInternal.CLASS_NAME;
  }

  static String getEdgeLinkFieldName(
      final ODirection direction,
      final String className,
      final boolean useVertexFieldsForEdgeLabels) {
    if (direction == null || direction == ODirection.BOTH) {
      throw new IllegalArgumentException("Direction not valid");
    }

    if (useVertexFieldsForEdgeLabels) {
      // PREFIX "out_" or "in_" TO THE FIELD NAME
      final String prefix =
          direction == ODirection.OUT ? DIRECTION_OUT_PREFIX : DIRECTION_IN_PREFIX;
      if (className == null || className.isEmpty() || className.equals(OEdgeInternal.CLASS_NAME)) {
        return prefix;
      }

      return prefix + className;
    } else
    // "out" or "in"
    {
      return direction == ODirection.OUT ? OEdgeInternal.DIRECTION_OUT : OEdgeInternal.DIRECTION_IN;
    }
  }

  /** updates old and new vertices connected to an edge after out/in update on the edge itself */
  static void changeVertexEdgePointers(
      ODocument edge,
      OIdentifiable prevInVertex,
      OIdentifiable currentInVertex,
      OIdentifiable prevOutVertex,
      OIdentifiable currentOutVertex) {
    var edgeClass = edge.getClassName();

    if (currentInVertex != prevInVertex) {
      changeVertexEdgePointersOneDirection(
          edge,
          prevInVertex,
          currentInVertex,
          prevOutVertex,
          currentOutVertex,
          edgeClass,
          ODirection.IN);
    }
    if (currentOutVertex != prevOutVertex) {
      changeVertexEdgePointersOneDirection(
          edge,
          prevOutVertex,
          currentOutVertex,
          prevInVertex,
          currentInVertex,
          edgeClass,
          ODirection.OUT);
    }
  }

  private static void changeVertexEdgePointersOneDirection(
      ODocument edge,
      OIdentifiable prevInVertex,
      OIdentifiable currentInVertex,
      OIdentifiable prevOutVertex,
      OIdentifiable currentOutVertex,
      String edgeClass,
      ODirection direction) {
    var prevOppositeVertex = prevOutVertex != null ? prevOutVertex : currentOutVertex;

    if (prevInVertex != null) {
      var inFieldName = OVertex.getEdgeLinkFieldName(direction, edgeClass);
      var prevRecord = prevInVertex.<ODocument>getRecord();

      var prevLink = prevRecord.getPropertyWithoutValidation(inFieldName);
      if (prevLink != null) {
        removeVertexLink(prevRecord, inFieldName, prevLink, edgeClass, edge);
      }

      var currentRecord = currentInVertex.<ODocument>getRecord();
      createLink(currentRecord, edge, inFieldName);

      var outFieldName = OVertex.getEdgeLinkFieldName(direction, edgeClass);
      var directLinkFieldName = OVertexInternal.getDirectEdgeLinkFieldName(outFieldName);

      var prevDirectLink = prevRecord.<ORidBag>getPropertyWithoutValidation(directLinkFieldName);

      if (prevDirectLink != null) {
        removeVertexLink(
            prevRecord, directLinkFieldName, prevDirectLink, edgeClass, prevOppositeVertex);
      }

      prevRecord.save();
      currentRecord.save();
    }

    var prevOppositeVertexDoc = prevOppositeVertex.<ODocument>getRecord();
    var outFieldName = OVertex.getEdgeLinkFieldName(direction.opposite(), edgeClass);
    var directLinkFieldName = OVertexInternal.getDirectEdgeLinkFieldName(outFieldName);

    var prevDirectLink =
        prevOppositeVertexDoc.<ORidBag>getPropertyWithoutValidation(directLinkFieldName);
    if (prevDirectLink != null) {
      removeVertexLink(
          prevOppositeVertexDoc, directLinkFieldName, prevDirectLink, edgeClass, prevInVertex);
    }

    var currentOppositeVertexDoc = currentOutVertex.<ODocument>getRecord();
    createLink(currentOppositeVertexDoc, currentInVertex, directLinkFieldName);

    currentOppositeVertexDoc.save();
  }

  private String[] resolveAliases(OSchema schema, String[] labels) {
    if (labels == null) {
      return null;
    }
    String[] result = new String[labels.length];

    for (int i = 0; i < labels.length; i++) {
      result[i] = resolveAlias(labels[i], schema);
    }

    return result;
  }

  private static String resolveAlias(String label, OSchema schema) {
    OClass clazz = schema.getClass(label);
    if (clazz != null) {
      return clazz.getName();
    }

    return label;
  }

  @Override
  default void deleteEdge(OVertex to, String label) {
    var db = (ODatabaseDocumentInternal) getDatabase();
    var schema = db.getMetadata().getImmutableSchemaSnapshot();
    OClass cl = schema.getClass(label);

    if (cl == null || !cl.isEdgeType()) {
      throw new IllegalArgumentException(label + " is not an edge class");
    }

    deleteEdge(to, cl);
  }

  @Override
  default void deleteEdge(OVertex to) {
    deleteEdge(to, OEdgeInternal.CLASS_NAME);
  }

  default void deleteEdge(OVertex to, OClass cls) {
    var db = (ODatabaseDocumentInternal) getDatabase();

    var label = cls.getName();
    var outFieldName = OVertex.getEdgeLinkFieldName(ODirection.OUT, label);
    var inFieldName = OVertex.getEdgeLinkFieldName(ODirection.IN, label);

    var from = getBaseDocument();
    var outLink = from.getPropertyWithoutValidation(outFieldName);

    var toInternal = (OVertexInternal) to;
    var inLink = toInternal.getPropertyWithoutValidation(inFieldName);

    if (inLink instanceof OIdentifiable edgeId && inLink.equals(outLink)) {
      // record edge
      assert db.getMetadata()
          .getSchema()
          .getClassByClusterId(edgeId.getIdentity().getClusterId())
          .isEdgeType();

      var edge = edgeId.<OElement>getRecord().toEdge();
      assert edge != null;

      // edge removal will cause detachments of links for vertices so we
      // should not do anything else it will be done automatically.
      edge.delete();

      return;
    }

    // it is lightweight edge,direct links were introduced after lightweight edges were deprecated
    // so we do not need to check their presence
    removeVertexLink(from, outFieldName, outLink, label, to);
    removeVertexLink(toInternal.getBaseDocument(), inFieldName, inLink, label, getIdentity());
  }

  private static void removeVertexLink(
      OElementInternal vertex,
      String fieldName,
      Object link,
      String label,
      OIdentifiable identifiable) {
    if (link instanceof Collection) {
      ((Collection<?>) link).remove(identifiable);
    } else if (link instanceof ORidBag) {
      ((ORidBag) link).remove(identifiable);
    } else if (link instanceof OIdentifiable && link.equals(vertex)) {
      vertex.removePropertyWithoutValidation(fieldName);
    } else {
      throw new IllegalArgumentException(
          label + " is not a valid link in vertex with rid " + vertex.getIdentity());
    }
  }

  /** Creates a link between a vertices and a Graph Element. */
  static void createLink(
      final ODocument fromVertex, final OIdentifiable to, final String fieldName) {
    final Object out;
    OType outType = fromVertex.fieldType(fieldName);
    Object found = fromVertex.getPropertyWithoutValidation(fieldName);

    final OClass linkClass = ODocumentInternal.getImmutableSchemaClass(fromVertex);
    if (linkClass == null) {
      throw new IllegalArgumentException("Class not found in source vertex: " + fromVertex);
    }

    final OProperty prop = linkClass.getProperty(fieldName);
    final OType propType = prop != null && prop.getType() != OType.ANY ? prop.getType() : null;

    if (found == null) {
      if (propType == OType.LINKLIST
          || (prop != null
              && "true".equalsIgnoreCase(prop.getCustom("ordered")))) { // TODO constant
        var coll = new ORecordLazyList(fromVertex);
        coll.add(to);
        out = coll;
        outType = OType.LINKLIST;
      } else if (propType == null || propType == OType.LINKBAG) {
        final ORidBag bag = new ORidBag();
        bag.add(to);
        out = bag;
        outType = OType.LINKBAG;
      } else if (propType == OType.LINK) {
        out = to;
        outType = OType.LINK;
      } else {
        throw new ODatabaseException(
            "Type of field provided in schema '"
                + prop.getType()
                + "' cannot be used for link creation.");
      }

    } else if (found instanceof OIdentifiable foundId) {
      if (prop != null && propType == OType.LINK) {
        throw new ODatabaseException(
            "Type of field provided in schema '"
                + prop.getType()
                + "' cannot be used for creation to hold several links.");
      }

      if (prop != null && "true".equalsIgnoreCase(prop.getCustom("ordered"))) { // TODO constant
        var coll = new ORecordLazyList(fromVertex);
        coll.add(foundId);
        coll.add(to);
        out = coll;
        outType = OType.LINKLIST;
      } else {
        final ORidBag bag = new ORidBag();
        bag.add(foundId);
        bag.add(to);
        out = bag;
        outType = OType.LINKBAG;
      }
    } else if (found instanceof ORidBag) {
      // ADD THE LINK TO THE COLLECTION
      out = null;

      ((ORidBag) found).add(to.getRecord());

    } else if (found instanceof Collection<?>) {
      // USE THE FOUND COLLECTION
      out = null;
      //noinspection unchecked
      ((Collection<OIdentifiable>) found).add(to);

    } else {
      throw new ODatabaseException(
          "Relationship content is invalid on field " + fieldName + ". Found: " + found);
    }

    if (out != null)
    // OVERWRITE IT
    {
      fromVertex.setPropertyWithoutValidation(fieldName, out, outType);
    }
  }

  private static void removeLinkFromEdge(ODocument vertex, OEdge edge, ODirection direction) {
    var schemaType = edge.getSchemaType();
    assert schemaType.isPresent();

    String className = schemaType.get().getName();
    OIdentifiable edgeId = ((OIdentifiable) edge).getIdentity();

    removeLinkFromEdge(
        vertex,
        edge,
        OVertex.getEdgeLinkFieldName(direction, className),
        edgeId,
        className,
        direction);
  }

  private static void removeLinkFromEdge(
      ODocument vertex,
      OEdge edge,
      String edgeField,
      OIdentifiable edgeId,
      String className,
      ODirection direction) {
    Object edgeProp = vertex.getPropertyWithoutValidation(edgeField);
    ORID oppositeVertexId = null;
    if (direction == ODirection.IN) {
      var fromIdentifiable = edge.getFromIdentifiable();
      if (fromIdentifiable != null) {
        oppositeVertexId = fromIdentifiable.getIdentity();
      }
    } else {
      var toIdentifiable = edge.getToIdentifiable();
      if (toIdentifiable != null) {
        oppositeVertexId = toIdentifiable.getIdentity();
      }
    }

    if (edgeId == null) {
      // lightweight edge
      edgeId = oppositeVertexId;
    }

    removeEdgeLinkFromProperty(vertex, edge, edgeField, edgeId, edgeProp);

    var directConnectionFiledName = getDirectEdgeLinkFieldName(edgeField);
    var directLink = vertex.getPropertyWithoutValidation(directConnectionFiledName);
    if (directLink != null && oppositeVertexId != null) {
      removeVertexLink(vertex, directConnectionFiledName, directLink, className, oppositeVertexId);
    }
  }

  private static void removeEdgeLinkFromProperty(
      ODocument vertex, OEdge edge, String edgeField, OIdentifiable edgeId, Object edgeProp) {
    if (edgeProp instanceof Collection) {
      ((Collection<?>) edgeProp).remove(edgeId);
    } else if (edgeProp instanceof ORidBag) {
      ((ORidBag) edgeProp).remove(edgeId);
    } else //noinspection deprecation
    if (edgeProp instanceof OIdentifiable
            && ((OIdentifiable) edgeProp).getIdentity() != null
            && ((OIdentifiable) edgeProp).getIdentity().equals(edgeId)
        || edge.isLightweight()) {
      vertex.removePropertyWithoutValidation(edgeField);
    } else {
      OLogManager.instance()
          .warn(
              vertex,
              "Error detaching edge: the vertex collection field is of type "
                  + (edgeProp == null ? "null" : edgeProp.getClass()));
    }
  }

  static void removeIncomingEdge(OVertex vertex, OEdge edge) {
    removeLinkFromEdge(((OVertexInternal) vertex).getBaseDocument(), edge, ODirection.IN);
  }

  static void removeOutgoingEdge(OVertex vertex, OEdge edge) {
    removeLinkFromEdge(((OVertexInternal) vertex).getBaseDocument(), edge, ODirection.OUT);
  }

  static String getDirectEdgeLinkFieldName(final String fieldName) {
    return fieldName + DIRECT_LINK_SUFFIX;
  }

  static void validateConnectionType(OVertex vertex, String className, String fieldName) {
    var fromClassOpt = vertex.getSchemaType();
    assert fromClassOpt.isPresent();
    var fromClass = fromClassOpt.get();

    var property = fromClass.getProperty(fieldName);
    if (property != null) {
      var propertyType = property.getType();
      if (propertyType != OType.LINKBAG
          && propertyType != OType.LINKLIST
          && propertyType != OType.LINKSET
          && propertyType != OType.LINK) {
        throw new IllegalStateException(
            "Property "
                + fieldName
                + " for edge "
                + className
                + " already defined and has type that can not be used for edge creation - "
                + propertyType);
      }
    }
  }
}
