/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Property;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import java.io.IOException;
import java.util.Map;

public class OServerCommandPostProperty extends OServerCommandAuthenticatedDbAbstract {

  private static final String PROPERTY_TYPE_JSON_FIELD = "propertyType";
  private static final String LINKED_CLASS_JSON_FIELD = "linkedClass";
  private static final String LINKED_TYPE_JSON_FIELD = "linkedType";
  private static final String[] NAMES = {"POST|property/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    try (DatabaseSessionInternal db = getProfiledDatabaseInstance(iRequest)) {
      if (iRequest.getContent() == null || iRequest.getContent().length() <= 0) {
        return addSingleProperty(iRequest, iResponse, db);
      } else {
        return addMultipreProperties(iRequest, iResponse, db);
      }
    }
  }

  @SuppressWarnings("unused")
  protected boolean addSingleProperty(
      final OHttpRequest iRequest, final OHttpResponse iResponse,
      final DatabaseSessionInternal db)
      throws InterruptedException, IOException {
    String[] urlParts =
        checkSyntax(
            iRequest.getUrl(),
            4,
            "Syntax error:"
                + " property/<database>/<class-name>/<property-name>/[<property-type>]/[<link-type>]");

    iRequest.getData().commandInfo = "Create property";
    iRequest.getData().commandDetail = urlParts[2] + "." + urlParts[3];

    if (db.getMetadata().getSchema().getClass(urlParts[2]) == null) {
      throw new IllegalArgumentException("Invalid class '" + urlParts[2] + "'");
    }

    final SchemaClass cls = db.getMetadata().getSchema().getClass(urlParts[2]);

    final String propertyName = urlParts[3];

    final PropertyType propertyType =
        urlParts.length > 4 ? PropertyType.valueOf(urlParts[4]) : PropertyType.STRING;

    switch (propertyType) {
      case LINKLIST:
      case LINKMAP:
      case LINKSET:
      case LINK: {
        /* try link as PropertyType */
        PropertyType linkType = null;
        SchemaClass linkClass = null;
        if (urlParts.length >= 6) {
          try {
            linkType = PropertyType.valueOf(urlParts[5]);
          } catch (IllegalArgumentException ex) {
          }

          if (linkType == null) {
            linkClass = db.getMetadata().getSchema().getClass(urlParts[5]);
            if (linkClass == null) {
              throw new IllegalArgumentException(
                  "linked type declared as "
                      + urlParts[5]
                      + " can be either a Type or a Class, use the JSON document usage instead."
                      + " See 'http://code.google.com/p/orient/w/edit/OrientDB_REST'");
            }
          }
        }

        if (linkType != null) {
          final Property prop = cls.createProperty(db, propertyName, propertyType, linkType);
        } else if (linkClass != null) {
          final Property prop = cls.createProperty(db, propertyName, propertyType, linkClass);
        } else {
          final Property prop = cls.createProperty(db, propertyName, propertyType);
        }
        break;
      }

      default:
        final Property prop = cls.createProperty(db, propertyName, propertyType);
        break;
    }

    iResponse.send(
        OHttpUtils.STATUS_CREATED_CODE,
        OHttpUtils.STATUS_CREATED_DESCRIPTION,
        OHttpUtils.CONTENT_TEXT_PLAIN,
        cls.properties(db).size(),
        null);

    return false;
  }

  @SuppressWarnings({"unchecked", "unused"})
  protected boolean addMultipreProperties(
      final OHttpRequest iRequest, final OHttpResponse iResponse,
      final DatabaseSessionInternal db)
      throws InterruptedException, IOException {
    String[] urlParts =
        checkSyntax(iRequest.getUrl(), 3, "Syntax error: property/<database>/<class-name>");

    iRequest.getData().commandInfo = "Create property";
    iRequest.getData().commandDetail = urlParts[2];

    if (db.getMetadata().getSchema().getClass(urlParts[2]) == null) {
      throw new IllegalArgumentException("Invalid class '" + urlParts[2] + "'");
    }

    final SchemaClass cls = db.getMetadata().getSchema().getClass(urlParts[2]);

    final EntityImpl propertiesDoc = new EntityImpl();
    propertiesDoc.fromJSON(iRequest.getContent());

    for (String propertyName : propertiesDoc.fieldNames()) {
      final Map<String, String> doc = propertiesDoc.field(propertyName);
      final PropertyType propertyType = PropertyType.valueOf(doc.get(PROPERTY_TYPE_JSON_FIELD));
      switch (propertyType) {
        case LINKLIST:
        case LINKMAP:
        case LINKSET: {
          final String linkType = doc.get(LINKED_TYPE_JSON_FIELD);
          final String linkClass = doc.get(LINKED_CLASS_JSON_FIELD);
          if (linkType != null) {
            final Property prop =
                cls.createProperty(db, propertyName, propertyType, PropertyType.valueOf(linkType));
          } else if (linkClass != null) {
            final Property prop =
                cls.createProperty(db,
                    propertyName, propertyType, db.getMetadata().getSchema().getClass(linkClass));
          } else {
            throw new IllegalArgumentException(
                "property named "
                    + propertyName
                    + " is declared as "
                    + propertyType
                    + " but linked type is not declared");
          }
          break;
        }
        case LINK: {
          final String linkClass = doc.get(LINKED_CLASS_JSON_FIELD);
          if (linkClass != null) {
            final Property prop =
                cls.createProperty(db,
                    propertyName, propertyType, db.getMetadata().getSchema().getClass(linkClass));
          } else {
            throw new IllegalArgumentException(
                "property named "
                    + propertyName
                    + " is declared as "
                    + propertyType
                    + " but linked Class is not declared");
          }
          break;
        }

        default:
          final Property prop = cls.createProperty(db, propertyName, propertyType);
          break;
      }
    }

    iResponse.send(
        OHttpUtils.STATUS_CREATED_CODE,
        OHttpUtils.STATUS_CREATED_DESCRIPTION,
        OHttpUtils.CONTENT_TEXT_PLAIN,
        cls.properties(db).size(),
        null);

    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
