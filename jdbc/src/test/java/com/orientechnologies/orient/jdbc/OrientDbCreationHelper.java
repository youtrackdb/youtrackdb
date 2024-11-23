/**
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * <p>*
 */
package com.orientechnologies.orient.jdbc;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.TimeZone;

public class OrientDbCreationHelper {

  public static void loadDB(ODatabaseSession db, int documents) throws IOException {

    db.begin();
    for (int i = 1; i <= documents; i++) {
      ODocument doc = new ODocument();
      doc.setClassName("Item");
      doc = createItem(i, doc);
      ((ODatabaseSessionInternal) db).save(doc, "Item");
    }

    createAuthorAndArticles(db, 50, 50);
    createArticleWithAttachmentSplitted(db);

    createWriterAndPosts(db, 10, 10);
    db.commit();
  }

  public static ODocument createItem(int id, ODocument doc) {
    String itemKey = Integer.valueOf(id).toString();

    doc.setClassName("Item");
    doc.field("stringKey", itemKey);
    doc.field("intKey", id);
    String contents =
        "OxygenDB is a deeply scalable Document-Graph DBMS with the flexibility of the Document"
            + " databases and the power to manage links of the Graph databases. It can work in"
            + " schema-less mode, schema-full or a mix of both. Supports advanced features such as"
            + " ACID Transactions, Fast Indexes, Native and SQL queries. It imports and exports"
            + " documents in JSON. Graphs of hundreads of linked documents can be retrieved all in"
            + " memory in few milliseconds without executing costly JOIN such as the Relational"
            + " DBMSs do. OxygenDB uses a new indexing algorithm called MVRB-Tree, derived from the"
            + " Red-Black Tree and from the B+Tree with benefits of both: fast insertion and ultra"
            + " fast lookup. The transactional engine can run in distributed systems supporting up"
            + " to 9.223.372.036 Billions of records for the maximum capacity of"
            + " 19.807.040.628.566.084 Terabytes of data distributed on multiple disks in multiple"
            + " nodes. OxygenDB is FREE for any use. Open Source License Apache 2.0. ";
    doc.field("text", contents);
    doc.field("title", "oxygenDB");
    doc.field("score", BigDecimal.valueOf(contents.length() / id));
    doc.field("length", contents.length(), OType.LONG);
    doc.field("published", (id % 2 > 0));
    doc.field("author", "anAuthor" + id);
    // doc.field("tags", asList("java", "orient", "nosql"),
    // OType.EMBEDDEDLIST);
    Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    instance.add(Calendar.HOUR_OF_DAY, -id);
    Date time = instance.getTime();
    doc.field("date", time, OType.DATE);
    doc.field("time", time, OType.DATETIME);

    return doc;
  }

  public static void createAuthorAndArticles(ODatabaseSession db, int totAuthors, int totArticles)
      throws IOException {
    int articleSerial = 0;
    for (int a = 1; a <= totAuthors; ++a) {
      ODocument author = new ODocument("Author");
      List<ODocument> articles = new ArrayList<>(totArticles);
      author.field("articles", articles);

      author.field("uuid", a, OType.DOUBLE);
      author.field("name", "Jay");
      author.field("rating", new Random().nextDouble());

      for (int i = 1; i <= totArticles; ++i) {
        ODocument article = new ODocument("Article");

        Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        Date time = instance.getTime();
        article.field("date", time, OType.DATE);

        article.field("uuid", articleSerial++);
        article.field("title", "the title for article " + articleSerial);
        article.field("content", "the content for article " + articleSerial);
        article.field("attachment", loadFile(db, "./src/test/resources/file.pdf"));

        articles.add(article);
      }

      author.save();
    }
  }

  public static ODocument createArticleWithAttachmentSplitted(ODatabaseSession db)
      throws IOException {

    ODocument article = new ODocument("Article");
    Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));

    Date time = instance.getTime();
    article.field("date", time, OType.DATE);

    article.field("uuid", 1000000);
    article.field("title", "the title 2");
    article.field("content", "the content 2");
    if (new File("./src/test/resources/file.pdf").exists()) {
      article.field("attachment", loadFile(db, "./src/test/resources/file.pdf", 256));
    }
    db.begin();
    db.save(article);
    db.commit();
    return article;
  }

  public static void createWriterAndPosts(ODatabaseSession db, int totAuthors, int totArticles)
      throws IOException {
    int articleSerial = 0;
    for (int a = 1; a <= totAuthors; ++a) {
      OVertex writer = db.newVertex("Writer");
      writer.setProperty("uuid", a);
      writer.setProperty("name", "happy writer");
      writer.setProperty("is_active", Boolean.TRUE);
      writer.setProperty("isActive", Boolean.TRUE);

      for (int i = 1; i <= totArticles; ++i) {

        OVertex post = db.newVertex("Post");

        Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        Date time = instance.getTime();
        post.setProperty("date", time, OType.DATE);
        post.setProperty("uuid", articleSerial++);
        post.setProperty("title", "the title");
        post.setProperty("content", "the content");

        db.newEdge(writer, post, "Writes");
      }
    }

    // additional wrong data
    OVertex writer = db.newVertex("Writer");
    writer.setProperty("uuid", totAuthors * 2);
    writer.setProperty("name", "happy writer");
    writer.setProperty("is_active", Boolean.TRUE);
    writer.setProperty("isActive", Boolean.TRUE);

    OVertex post = db.newVertex("Post");

    // no date!!

    post.setProperty("uuid", articleSerial * 2);
    post.setProperty("title", "the title");
    post.setProperty("content", "the content");

    db.newEdge(writer, post, "Writes");
  }

  private static OBlob loadFile(ODatabaseSession database, String filePath) throws IOException {
    final File f = new File(filePath);
    if (f.exists()) {
      BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(f));
      OBlob record = new ORecordBytes();
      record.fromInputStream(inputStream);
      return record;
    }

    return null;
  }

  private static List<ORID> loadFile(ODatabaseSession database, String filePath, int bufferSize)
      throws IOException {
    File binaryFile = new File(filePath);
    long binaryFileLength = binaryFile.length();
    int numberOfRecords = (int) (binaryFileLength / bufferSize);
    int remainder = (int) (binaryFileLength % bufferSize);
    if (remainder > 0) {
      numberOfRecords++;
    }
    List<ORID> binaryChuncks = new ArrayList<>(numberOfRecords);
    BufferedInputStream binaryStream = new BufferedInputStream(new FileInputStream(binaryFile));

    for (int i = 0; i < numberOfRecords; i++) {
      var index = i;
      var recnum = numberOfRecords;

      database.executeInTx(
          () -> {
            byte[] chunk;
            if (index == recnum - 1) {
              chunk = new byte[remainder];
            } else {
              chunk = new byte[bufferSize];
            }
            try {
              binaryStream.read(chunk);
            } catch (IOException e) {
              throw new RuntimeException(e);
            }

            OBlob recordChunk = new ORecordBytes(chunk);

            database.save(recordChunk);
            binaryChuncks.add(recordChunk.getIdentity());
          });
    }

    return binaryChuncks;
  }

  public static void createSchemaDB(ODatabaseSession db) {

    OSchema schema = db.getMetadata().getSchema();

    // item
    OClass item = schema.createClass("Item");

    item.createProperty(db, "stringKey", OType.STRING).createIndex(db, INDEX_TYPE.UNIQUE);
    item.createProperty(db, "intKey", OType.INTEGER).createIndex(db, INDEX_TYPE.UNIQUE);
    item.createProperty(db, "date", OType.DATE).createIndex(db, INDEX_TYPE.NOTUNIQUE);
    item.createProperty(db, "time", OType.DATETIME).createIndex(db, INDEX_TYPE.NOTUNIQUE);
    item.createProperty(db, "text", OType.STRING);
    item.createProperty(db, "score", OType.DECIMAL);
    item.createProperty(db, "length", OType.INTEGER).createIndex(db, INDEX_TYPE.NOTUNIQUE);
    item.createProperty(db, "published", OType.BOOLEAN).createIndex(db, INDEX_TYPE.NOTUNIQUE);
    item.createProperty(db, "title", OType.STRING).createIndex(db, INDEX_TYPE.NOTUNIQUE);
    item.createProperty(db, "author", OType.STRING).createIndex(db, INDEX_TYPE.NOTUNIQUE);
    item.createProperty(db, "tags", OType.EMBEDDEDLIST);

    // class Article
    OClass article = schema.createClass("Article");

    article.createProperty(db, "uuid", OType.LONG).createIndex(db, INDEX_TYPE.UNIQUE);
    article.createProperty(db, "date", OType.DATE).createIndex(db, INDEX_TYPE.NOTUNIQUE);
    article.createProperty(db, "title", OType.STRING);
    article.createProperty(db, "content", OType.STRING);
    // article.createProperty("attachment", OType.LINK);

    // author
    OClass author = schema.createClass("Author");

    author.createProperty(db, "uuid", OType.LONG).createIndex(db, INDEX_TYPE.UNIQUE);
    author.createProperty(db, "name", OType.STRING).setMin("3");
    author.createProperty(db, "rating", OType.DOUBLE);
    author.createProperty(db, "articles", OType.LINKLIST, article);

    // link article-->author
    article.createProperty(db, "author", OType.LINK, author);

    // Graph

    OClass v = schema.getClass("V");
    if (v == null) {
      schema.createClass("V");
    }

    OClass post = schema.createClass("Post", v);
    post.createProperty(db, "uuid", OType.LONG);
    post.createProperty(db, "title", OType.STRING);
    post.createProperty(db, "date", OType.DATE).createIndex(db, INDEX_TYPE.NOTUNIQUE);
    post.createProperty(db, "content", OType.STRING);

    OClass writer = schema.createClass("Writer", v);
    writer.createProperty(db, "uuid", OType.LONG).createIndex(db, INDEX_TYPE.UNIQUE);
    writer.createProperty(db, "name", OType.STRING);
    writer.createProperty(db, "is_active", OType.BOOLEAN);
    writer.createProperty(db, "isActive", OType.BOOLEAN);

    OClass e = schema.getClass("E");
    if (e == null) {
      schema.createClass("E");
    }

    schema.createClass("Writes", e);
  }
}
