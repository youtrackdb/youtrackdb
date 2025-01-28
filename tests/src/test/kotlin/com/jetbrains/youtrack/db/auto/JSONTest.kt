/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jetbrains.youtrack.db.auto

import com.jetbrains.youtrack.db.api.record.Entity
import com.jetbrains.youtrack.db.api.schema.PropertyType
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityHelper
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl
import org.testng.Assert
import org.testng.annotations.BeforeClass
import org.testng.annotations.Optional
import org.testng.annotations.Parameters
import org.testng.annotations.Test
import java.util.*

@Test
class JSONTest @Parameters(value = ["remote"]) constructor(@Optional remote: Boolean?) :
    BaseDBTest(remote != null && remote) {
    @BeforeClass
    @Throws(Exception::class)
    override fun beforeClass() {
        super.beforeClass()
        addBarackObamaAndFollowers()
    }

    @Test
    fun testAlmostLink() {
        db.executeInTx {
            val doc =
                (db.newEntity() as EntityImpl)
            doc.updateFromJSON("{\"title\": \"#330: Dollar Coins Are Done\"}")
        }
    }

    @Test
    fun testNullList() {
        db.executeInTx {
            val documentSource =
                (db.newEntity() as EntityImpl)
            documentSource.updateFromJSON("{\"list\" : [\"string\", null]}")

            val documentTarget =
                (db.newEntity() as EntityImpl)
            RecordInternal.unsetDirty(documentTarget)
            documentTarget.fromStream(documentSource.toStream())

            val list =
                documentTarget.field<TrackedList<Any>>("list", PropertyType.EMBEDDEDLIST)
            Assert.assertEquals(list[0], "string")
            Assert.assertNull(list[1])
        }
    }

    @Test
    fun testBooleanList() {
        db.executeInTx {
            val documentSource =
                (db.newEntity() as EntityImpl)
            documentSource.updateFromJSON("{\"list\" : [true, false]}")

            val documentTarget =
                (db.newEntity() as EntityImpl)
            RecordInternal.unsetDirty(documentTarget)
            documentTarget.fromStream(documentSource.toStream())

            val list =
                documentTarget.field<TrackedList<Any>>("list", PropertyType.EMBEDDEDLIST)
            Assert.assertEquals(list[0], true)
            Assert.assertEquals(list[1], false)
        }
    }

    @Test
    fun testNumericIntegerList() {
        db.executeInTx {
            val documentSource =
                (db.newEntity() as EntityImpl)
            documentSource.updateFromJSON("{\"list\" : [17,42]}")

            val documentTarget =
                (db.newEntity() as EntityImpl)
            RecordInternal.unsetDirty(documentTarget)
            documentTarget.fromStream(documentSource.toStream())

            val list =
                documentTarget.field<TrackedList<Any>>("list", PropertyType.EMBEDDEDLIST)
            Assert.assertEquals(list[0], 17)
            Assert.assertEquals(list[1], 42)
        }
    }

    @Test
    fun testNumericLongList() {
        db.executeInTx {
            val documentSource =
                (db.newEntity() as EntityImpl)
            documentSource.updateFromJSON("{\"list\" : [100000000000,100000000001]}")

            val documentTarget =
                (db.newEntity() as EntityImpl)
            RecordInternal.unsetDirty(documentTarget)
            documentTarget.fromStream(documentSource.toStream())

            val list =
                documentTarget.field<TrackedList<Any>>("list", PropertyType.EMBEDDEDLIST)
            Assert.assertEquals(list[0], 100000000000L)
            Assert.assertEquals(list[1], 100000000001L)
        }
    }

    @Test
    fun testNumericFloatList() {
        db.executeInTx {
            val documentSource =
                (db.newEntity() as EntityImpl)
            documentSource.updateFromJSON("{\"list\" : [17.3,42.7]}")

            val documentTarget =
                (db.newEntity() as EntityImpl)
            RecordInternal.unsetDirty(documentTarget)
            documentTarget.fromStream(documentSource.toStream())

            val list =
                documentTarget.field<TrackedList<Any>>("list", PropertyType.EMBEDDEDLIST)
            Assert.assertEquals(list[0], 17.3f)
            Assert.assertEquals(list[1], 42.7f)
        }
    }

    @Test
    fun testNullity() {
        val (map, rid) = db.computeInTx {
            val entity = db.newEntity()
            entity.updateFromJSON(
                "{\"gender\":{\"name\":\"Male\"},\"firstName\":\"Jack\",\"lastName\":\"Williams\"," +
                        "\"phone\":\"561-401-3348\",\"email\":\"0586548571@example.com\",\"address\":{\"street1\":\"Smith" +
                        " Ave\"," +
                        "\"street2\":null,\"city\":\"GORDONSVILLE\",\"state\":\"VA\",\"code\":\"22942\"},\"dob\":\"2011-11-17" +
                        " 03:17:04\"}"
            )
            Pair(entity.toMap(), entity.identity)
        }

        db.executeInTx {
            val loadedEntity = db.loadEntity(rid)
            val loadedMap = loadedEntity.toMap()
            Assert.assertEquals(loadedMap, map)
        }
    }

    @Test
    fun testNanNoTypes() {
        val rid1 = db.computeInTx {
            val entity = db.newEntity()
            entity.setProperty("nan", Double.NaN)
            entity.setProperty("p_infinity", Double.POSITIVE_INFINITY)
            entity.setProperty("n_infinity", Double.NEGATIVE_INFINITY)
            entity.identity
        }

        val (map1, json1) = db.computeInTx {
            val entity = db.loadEntity(rid1)
            Pair(entity.toMap(), entity.toJSON())
        }

        db.executeInTx {
            val entity = db.entityFromJson(json1)
            Assert.assertTrue(entity.isDirty)
            Assert.assertEquals(entity.toMap(), map1)
        }


        val rid2 = db.computeInTx {
            val entity = db.newEntity()
            entity.setProperty("nan", Float.NaN)
            entity.setProperty("p_infinity", Float.POSITIVE_INFINITY)
            entity.setProperty("n_infinity", Float.NEGATIVE_INFINITY)

            entity.identity
        }

        val (map2, json2) = db.computeInTx {
            val entity = db.loadEntity(rid2)
            Pair(entity.toMap(), entity.toJSON())
        }

        db.executeInTx {
            val entity = db.entityFromJson(json2)
            Assert.assertTrue(entity.isDirty)
            Assert.assertEquals(entity.toMap(), map2)
        }
    }

    @Test
    fun testEmbeddedList() {
        val rid = db.computeInTx {
            val original = db.newEntity()
            val list = original.getOrCreateEmbeddedList<Entity>("embeddedList")

            val entityOne = db.newEntity()
            entityOne.setProperty("name", "Luca")
            list.add(entityOne)

            val entityTwo = db.newEntity()
            entityTwo.setProperty("name", "Marcus")
            list.add(entityTwo)
            original.identity
        }

        db.executeInTx {
            val original = db.loadEntity(rid)
            val json = original.toJSON(FORMAT_WITHOUT_RID)
            val restoredEntity = db.entityFromJson(json)

            val originalMap = original.toMap()
            val restoredMap = restoredEntity.toMap()

            originalMap.remove(EntityHelper.ATTRIBUTE_RID)
            restoredMap.remove(EntityHelper.ATTRIBUTE_RID)

            Assert.assertEquals(originalMap, restoredMap)
        }
    }

    @Test
    fun testEmbeddedMap() {
        val rid = db.computeInTx {
            val original = db.newEntity()
            val map = original.getOrCreateEmbeddedMap<Entity>("embeddedMap")

            val entityOne = db.newEntity()
            entityOne.setProperty("name", "Luca")
            map["Luca"] = entityOne

            val entityTwo = db.newEntity()
            entityTwo.setProperty("name", "Marcus")
            map["Marcus"] = entityTwo

            val entityThree = db.newEntity()
            entityThree.setProperty("name", "Cesare")
            map["Cesare"] = entityThree

            original.identity
        }

        db.executeInTx {
            val original = db.loadEntity(rid)
            val json = original.toJSON(FORMAT_WITHOUT_RID)

            val loadedDoc = db.newEntity()
            loadedDoc.updateFromJSON(json)

            val originalMap = original.toMap()
            val loadedMap = loadedDoc.toMap()

            originalMap.remove(EntityHelper.ATTRIBUTE_RID)
            loadedMap.remove(EntityHelper.ATTRIBUTE_RID)

            Assert.assertEquals(originalMap, loadedMap)
        }
    }
//
//    @Test
//    fun testListToJSON() {
//        val list: MutableList<EntityImpl> = ArrayList()
//        val first = (db.newEntity() as EntityImpl).field("name", "Luca")
//        val second = (db.newEntity() as EntityImpl).field("name", "Marcus")
//        list.add(first)
//        list.add(second)
//
//        val jsonResult = JSONWriter.listToJSON(db, list, null)
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{\"result\": $jsonResult}")
//        val result = doc.field<Collection<EntityImpl>>("result")
//        Assert.assertTrue(result is Collection<*>)
//        Assert.assertEquals(result.size, 2)
//        for (resultDoc in result) {
//            Assert.assertTrue(first.hasSameContentOf(resultDoc) || second.hasSameContentOf(resultDoc))
//        }
//    }
//
//    @Test
//    fun testEmptyEmbeddedMap() {
//        val doc = (db.newEntity() as EntityImpl)
//
//        val map: Map<String, EntityImpl> = HashMap()
//        doc.field("embeddedMap", map, PropertyType.EMBEDDEDMAP)
//
//        val json = doc.toJSON()
//        val loadedDoc = (db.newEntity() as EntityImpl)
//        loadedDoc.updateFromJSON(json)
//        Assert.assertTrue(doc.hasSameContentOf(loadedDoc))
//        Assert.assertTrue(loadedDoc.containsField("embeddedMap"))
//        Assert.assertTrue(loadedDoc.field<Any>("embeddedMap") is Map<*, *>)
//
//        val loadedMap = loadedDoc.field<Map<String, EntityImpl>>("embeddedMap")
//        Assert.assertEquals(loadedMap.size, 0)
//    }
//
//    @Test
//    fun testMultiLevelTypes() {
//        val oldDataTimeFormat = db[DatabaseSession.ATTRIBUTES.DATE_TIME_FORMAT].toString()
//        db[DatabaseSession.ATTRIBUTES.DATE_TIME_FORMAT] =
//            StorageConfiguration.DEFAULT_DATETIME_FORMAT
//        try {
//            val newDoc = (db.newEntity() as EntityImpl)
//            newDoc.field("long", 100000000000L)
//            newDoc.field("date", Date())
//            newDoc.field("byte", 12.toByte())
//            val firstLevelDoc = (db.newEntity() as EntityImpl)
//            firstLevelDoc.field("long", 200000000000L)
//            firstLevelDoc.field("date", Date())
//            firstLevelDoc.field("byte", 13.toByte())
//            val secondLevelDoc = (db.newEntity() as EntityImpl)
//            secondLevelDoc.field("long", 300000000000L)
//            secondLevelDoc.field("date", Date())
//            secondLevelDoc.field("byte", 14.toByte())
//            val thirdLevelDoc = (db.newEntity() as EntityImpl)
//            thirdLevelDoc.field("long", 400000000000L)
//            thirdLevelDoc.field("date", Date())
//            thirdLevelDoc.field("byte", 15.toByte())
//            newDoc.field("doc", firstLevelDoc)
//            firstLevelDoc.field("doc", secondLevelDoc)
//            secondLevelDoc.field("doc", thirdLevelDoc)
//
//            val json = newDoc.toJSON()
//            val loadedDoc = (db.newEntity() as EntityImpl)
//            loadedDoc.updateFromJSON(json)
//
//            Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc))
//            Assert.assertTrue(loadedDoc.field<Any>("long") is Long)
//            Assert.assertEquals(
//                (newDoc.field<Any>("long") as Long), (loadedDoc.field<Any>("long") as Long)
//            )
//            Assert.assertTrue(loadedDoc.field<Any>("date") is Date)
//            Assert.assertTrue(loadedDoc.field<Any>("byte") is Byte)
//            Assert.assertEquals(
//                (newDoc.field<Any>("byte") as Byte), (loadedDoc.field<Any>("byte") as Byte)
//            )
//            Assert.assertTrue(loadedDoc.field<Any>("doc") is EntityImpl)
//
//            val firstDoc = loadedDoc.field<EntityImpl>("doc")
//            Assert.assertTrue(firstLevelDoc.hasSameContentOf(firstDoc))
//            Assert.assertTrue(firstDoc.field<Any>("long") is Long)
//            Assert.assertEquals(
//                (firstLevelDoc.field<Any>("long") as Long),
//                (firstDoc.field<Any>("long") as Long)
//            )
//            Assert.assertTrue(firstDoc.field<Any>("date") is Date)
//            Assert.assertTrue(firstDoc.field<Any>("byte") is Byte)
//            Assert.assertEquals(
//                (firstLevelDoc.field<Any>("byte") as Byte),
//                (firstDoc.field<Any>("byte") as Byte)
//            )
//            Assert.assertTrue(firstDoc.field<Any>("doc") is EntityImpl)
//
//            val secondDoc = firstDoc.field<EntityImpl>("doc")
//            Assert.assertTrue(secondLevelDoc.hasSameContentOf(secondDoc))
//            Assert.assertTrue(secondDoc.field<Any>("long") is Long)
//            Assert.assertEquals(
//                (secondLevelDoc.field<Any>("long") as Long),
//                (secondDoc.field<Any>("long") as Long)
//            )
//            Assert.assertTrue(secondDoc.field<Any>("date") is Date)
//            Assert.assertTrue(secondDoc.field<Any>("byte") is Byte)
//            Assert.assertEquals(
//                (secondLevelDoc.field<Any>("byte") as Byte),
//                (secondDoc.field<Any>("byte") as Byte)
//            )
//            Assert.assertTrue(secondDoc.field<Any>("doc") is EntityImpl)
//
//            val thirdDoc = secondDoc.field<EntityImpl>("doc")
//            Assert.assertTrue(thirdLevelDoc.hasSameContentOf(thirdDoc))
//            Assert.assertTrue(thirdDoc.field<Any>("long") is Long)
//            Assert.assertEquals(
//                (thirdLevelDoc.field<Any>("long") as Long),
//                (thirdDoc.field<Any>("long") as Long)
//            )
//            Assert.assertTrue(thirdDoc.field<Any>("date") is Date)
//            Assert.assertTrue(thirdDoc.field<Any>("byte") is Byte)
//            Assert.assertEquals(
//                (thirdLevelDoc.field<Any>("byte") as Byte),
//                (thirdDoc.field<Any>("byte") as Byte)
//            )
//        } finally {
//            db[DatabaseSession.ATTRIBUTES.DATE_TIME_FORMAT] = oldDataTimeFormat
//        }
//    }
//
//    @Test
//    fun testMerge() {
//        val doc1 = (db.newEntity() as EntityImpl)
//        val list = ArrayList<String>()
//        doc1.field("embeddedList", list, PropertyType.EMBEDDEDLIST)
//        list.add("Luca")
//        list.add("Marcus")
//        list.add("Jay")
//        doc1.field("salary", 10000)
//        doc1.field("years", 16)
//
//        val doc2 = (db.newEntity() as EntityImpl)
//        val list2 = ArrayList<String>()
//        doc2.field("embeddedList", list2, PropertyType.EMBEDDEDLIST)
//        list2.add("Luca")
//        list2.add("Michael")
//        doc2.field("years", 32)
//
//        val docMerge1 = doc1.copy()
//        docMerge1.merge(doc2, true, true)
//
//        Assert.assertTrue(docMerge1.containsField("embeddedList"))
//        Assert.assertTrue(docMerge1.field<Any>("embeddedList") is List<*>)
//        Assert.assertEquals((docMerge1.field<Any>("embeddedList") as List<*>).size, 4)
//        Assert.assertTrue(
//            (docMerge1.field<Any>("embeddedList") as List<*>).first() is String
//        )
//        Assert.assertEquals((docMerge1.field<Any>("salary") as Int), 10000)
//        Assert.assertEquals((docMerge1.field<Any>("years") as Int), 32)
//
//        val docMerge2 = doc1.copy()
//        docMerge2.merge(doc2, true, false)
//
//        Assert.assertTrue(docMerge2.containsField("embeddedList"))
//        Assert.assertTrue(docMerge2.field<Any>("embeddedList") is List<*>)
//        Assert.assertEquals((docMerge2.field<Any>("embeddedList") as List<*>).size, 2)
//        Assert.assertTrue(
//            (docMerge2.field<Any>("embeddedList") as List<*>).first() is String
//        )
//        Assert.assertEquals((docMerge2.field<Any>("salary") as Int), 10000)
//        Assert.assertEquals((docMerge2.field<Any>("years") as Int), 32)
//
//        val docMerge3 = doc1.copy()
//
//        doc2.removeField("years")
//        docMerge3.merge(doc2, false, false)
//
//        Assert.assertTrue(docMerge3.containsField("embeddedList"))
//        Assert.assertTrue(docMerge3.field<Any>("embeddedList") is List<*>)
//        Assert.assertEquals((docMerge3.field<Any>("embeddedList") as List<*>).size, 2)
//        Assert.assertTrue(
//            (docMerge3.field<Any>("embeddedList") as List<*>).first() is String
//        )
//        Assert.assertFalse(docMerge3.containsField("salary"))
//        Assert.assertFalse(docMerge3.containsField("years"))
//    }
//
//    @Test
//    fun testNestedEmbeddedMap() {
//        val newDoc = (db.newEntity() as EntityImpl)
//
//        val map1 = mutableMapOf<String, Map<String, Any>>()
//        newDoc.field("map1", map1, PropertyType.EMBEDDEDMAP)
//
//        val map2 = mutableMapOf<String, Map<String, Any>>()
//        map1["map2"] = map2
//
//        val map3: Map<String, HashMap<String, *>> = HashMap()
//        map2["map3"] = map3 as HashMap<String, *>
//
//        val json = newDoc.toJSON()
//        val loadedDoc = (db.newEntity() as EntityImpl)
//        loadedDoc.updateFromJSON(json)
//
//        Assert.assertTrue(newDoc.hasSameContentOf(loadedDoc))
//
//        Assert.assertTrue(loadedDoc.containsField("map1"))
//        Assert.assertTrue(loadedDoc.field<Any>("map1") is Map<*, *>)
//        val loadedMap1 = loadedDoc.field<Map<String, EntityImpl>>("map1")
//        Assert.assertEquals(loadedMap1.size, 1)
//
//        Assert.assertTrue(loadedMap1.containsKey("map2"))
//        Assert.assertTrue(loadedMap1["map2"] is Map<*, *>)
//        val loadedMap2 = loadedMap1["map2"] as Map<*, *>?
//        Assert.assertEquals(loadedMap2!!.size, 1)
//
//        Assert.assertTrue(loadedMap2.containsKey("map3"))
//        Assert.assertTrue(loadedMap2["map3"] is Map<*, *>)
//        val loadedMap3 = loadedMap2["map3"] as Map<*, *>?
//        Assert.assertEquals(loadedMap3!!.size, 0)
//    }
//
//    @Test
//    fun testFetchedJson() {
//        val resultSet =
//            db
//                .command("select * from Profile where name = 'Barack' and surname = 'Obama'")
//                .toList()
//
//        for (result in resultSet) {
//            val jsonFull =
//                result.asEntity()!!
//                    .toJSON("type,rid,version,class,keepTypes,attribSameRow,indent:0,fetchPlan:*:-1")
//            val loadedDoc = (db.newEntity() as EntityImpl)
//            loadedDoc.updateFromJSON(jsonFull)
//            Assert.assertTrue((result.asEntity() as EntityImpl).hasSameContentOf(loadedDoc))
//        }
//    }
//
//    // Requires JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES
//    fun testSpecialChar() {
//        var doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON(
//            "{name:{\"%Field\":[\"value1\",\"value2\"],\"%Field2\":{},\"%Field3\":\"value3\"}}"
//        )
//        db.begin()
//        doc.save()
//        db.commit()
//
//        db.begin()
//        doc = db.bindToSession(doc)
//
//        val map = doc.toMap()
//        val docToCompare = (db.newEntity() as EntityImpl)
//        docToCompare.updateFromMap(map)
//
//        val loadedDoc = db.load<EntityImpl>(doc.identity)
//
//        Assert.assertTrue(
//            docToCompare.hasSameContentOf(loadedDoc)
//        )
//        db.commit()
//    }
//
//    fun testArrayOfArray() {
//        var doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON(
//            "{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ [ 100,"
//                    + "  0 ],  [ 101, 1 ] ]}"
//        )
//
//        db.begin()
//        doc.save()
//        db.commit()
//        db.begin()
//        doc = db.bindToSession(doc)
//
//        val map = doc.toMap()
//        val docToCompare = (db.newEntity() as EntityImpl)
//        docToCompare.updateFromMap(map)
//
//        val loadedDoc = db.load<EntityImpl>(doc.identity)
//        Assert.assertTrue(docToCompare.hasSameContentOf(loadedDoc))
//    }
//
//    fun testLongTypes() {
//        var doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON(
//            "{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ ["
//                    + " 32874387347347,  0 ],  [ -23736753287327, 1 ] ]}"
//        )
//
//        db.begin()
//        doc.save()
//        db.commit()
//
//        db.begin()
//        doc = db.bindToSession(doc)
//        val map = doc.toMap()
//        val docToCompare = (db.newEntity() as EntityImpl)
//        docToCompare.updateFromMap(map)
//
//        val loadedDoc = db.load<EntityImpl>(doc.identity)
//        Assert.assertTrue(docToCompare.hasSameContentOf(loadedDoc))
//        db.commit()
//    }
//
//    // Requires JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES
//    fun testSpecialChars() {
//        var doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON(
//            "{Field:{\"Key1\":[\"Value1\",\"Value2\"],\"Key2\":{\"%%dummy%%\":null},\"Key3\":\"Value3\"}}"
//        )
//        db.begin()
//        doc.save()
//        db.commit()
//        db.begin()
//        doc = db.bindToSession(doc)
//
//        val map = doc.toMap()
//        val docToCompare = (db.newEntity() as EntityImpl)
//        docToCompare.updateFromMap(map)
//
//        val loadedDoc = db.load<EntityImpl>(doc.identity)
//        Assert.assertEquals(docToCompare, loadedDoc)
//
//        db.commit()
//    }
//
//    fun testJsonToStream() {
//        val doc1Json =
//            "{Key1:{\"%Field1\":[{},{},{},{},{}],\"%Field2\":false,\"%Field3\":\"Value1\"}}"
//        val doc1 = EntityImpl(db)
//        doc1.updateFromJSON(doc1Json)
//        val doc1String = String(
//            RecordSerializerSchemaAware2CSV.INSTANCE.toStream(db, doc1)
//        )
//        Assert.assertEquals("{$doc1String}", doc1Json)
//
//        val doc2Json =
//            "{Key1:{\"%Field1\":[{},{},{},{},{}],\"%Field2\":false,\"%Field3\":\"Value1\"}}"
//        val doc2 = EntityImpl(db)
//        doc2.updateFromJSON(doc2Json)
//        val doc2String = String(
//            RecordSerializerSchemaAware2CSV.INSTANCE.toStream(db, doc2)
//        )
//        Assert.assertEquals("{$doc2String}", doc2Json)
//    }
//
//    fun testSameNameCollectionsAndMap() {
//        var doc = (db.newEntity() as EntityImpl)
//        doc.field("string", "STRING_VALUE")
//        var list: MutableList<EntityImpl?> = ArrayList()
//        for (i in 0..0) {
//            val doc1 = (db.newEntity() as EntityImpl)
//            doc.field("number", i)
//            list.add(doc1)
//            val docMap: MutableMap<String, EntityImpl> = HashMap()
//            for (j in 0..0) {
//                val doc2 = (db.newEntity() as EntityImpl)
//                doc2.field("blabla", j)
//                docMap[j.toString()] = doc2
//                val doc3 = (db.newEntity() as EntityImpl)
//                doc3.field("blubli", 0.toString())
//                doc2.field("out", doc3)
//            }
//            doc1.field("out", docMap)
//            list.add(doc1)
//        }
//        doc.field("out", list)
//
//        var json = doc.toJSON()
//        var newDoc = (db.newEntity() as EntityImpl)
//        newDoc.updateFromJSON(json)
//
//        Assert.assertEquals(json, newDoc.toJSON())
//        Assert.assertTrue(newDoc.hasSameContentOf(doc))
//
//        doc = (db.newEntity() as EntityImpl)
//        doc.field("string", "STRING_VALUE")
//        val docMap: MutableMap<String, EntityImpl> = HashMap()
//        for (i in 0..9) {
//            val doc1 = (db.newEntity() as EntityImpl)
//            doc.field("number", i)
//            list.add(doc1)
//            list = ArrayList()
//            for (j in 0..4) {
//                val doc2 = (db.newEntity() as EntityImpl)
//                doc2.field("blabla", j)
//                list.add(doc2)
//                val doc3 = (db.newEntity() as EntityImpl)
//                doc3.field("blubli", (i + j).toString())
//                doc2.field("out", doc3)
//            }
//            doc1.field("out", list)
//            docMap[i.toString()] = doc1
//        }
//        doc.field("out", docMap)
//        json = doc.toJSON()
//        newDoc = (db.newEntity() as EntityImpl)
//        newDoc.updateFromJSON(json)
//        Assert.assertEquals(newDoc.toJSON(), json)
//        Assert.assertTrue(newDoc.hasSameContentOf(doc))
//    }
//
//    fun testSameNameCollectionsAndMap2() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.field("string", "STRING_VALUE")
//        val list: MutableList<EntityImpl> = ArrayList()
//        for (i in 0..1) {
//            val doc1 = (db.newEntity() as EntityImpl)
//            list.add(doc1)
//            val docMap: MutableMap<String, EntityImpl> = HashMap()
//            for (j in 0..4) {
//                val doc2 = (db.newEntity() as EntityImpl)
//                doc2.field("blabla", j)
//                docMap[j.toString()] = doc2
//            }
//            doc1.field("theMap", docMap)
//            list.add(doc1)
//        }
//        doc.field("theList", list)
//        val json = doc.toJSON()
//        val newDoc = (db.newEntity() as EntityImpl)
//        newDoc.updateFromJSON(json)
//        Assert.assertEquals(newDoc.toJSON(), json)
//        Assert.assertTrue(newDoc.hasSameContentOf(doc))
//    }
//
//    fun testSameNameCollectionsAndMap3() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.field("string", "STRING_VALUE")
//        val list: MutableList<Map<String, EntityImpl>> = ArrayList()
//        for (i in 0..1) {
//            val docMap: MutableMap<String, EntityImpl> = HashMap()
//            for (j in 0..4) {
//                val doc1 = (db.newEntity() as EntityImpl)
//                doc1.field("blabla", j)
//                docMap[j.toString()] = doc1
//            }
//
//            list.add(docMap)
//        }
//        doc.field("theList", list)
//        val json = doc.toJSON()
//        val newDoc = (db.newEntity() as EntityImpl)
//        newDoc.updateFromJSON(json)
//        Assert.assertEquals(newDoc.toJSON(), json)
//    }
//
//    fun testNestedJsonCollection() {
//        if (!db.metadata.schema.existsClass("Device")) {
//            db.metadata.schema.createClass("Device")
//        }
//
//        db.begin()
//        db
//            .command(
//                "insert into device (resource_id, domainset) VALUES (0, [ { 'domain' : 'abc' }, {"
//                        + " 'domain' : 'pqr' } ])"
//            )
//            .close()
//        db.commit()
//
//        var result = db.query("select from device where domainset.domain contains 'abc'")
//        Assert.assertTrue(result.stream().findAny().isPresent)
//
//        result = db.query("select from device where domainset[domain = 'abc'] is not null")
//        Assert.assertTrue(result.stream().findAny().isPresent)
//
//        result = db.query("select from device where domainset.domain contains 'pqr'")
//        Assert.assertTrue(result.stream().findAny().isPresent)
//    }
//
//    fun testNestedEmbeddedJson() {
//        if (!db.metadata.schema.existsClass("Device")) {
//            db.metadata.schema.createClass("Device")
//        }
//
//        db.begin()
//        db
//            .command("insert into device (resource_id, domainset) VALUES (1, { 'domain' : 'eee' })")
//            .close()
//        db.commit()
//
//        val result = db.query("select from device where domainset.domain = 'eee'")
//        Assert.assertTrue(result.stream().findAny().isPresent)
//    }
//
//    fun testNestedMultiLevelEmbeddedJson() {
//        if (!db.metadata.schema.existsClass("Device")) {
//            db.metadata.schema.createClass("Device")
//        }
//
//        db.begin()
//        db
//            .command(
//                "insert into device (domainset) values ({'domain' : { 'lvlone' : { 'value' : 'five' } }"
//                        + " } )"
//            )
//            .close()
//        db.commit()
//
//        val result =
//            db.query("select from device where domainset.domain.lvlone.value = 'five'")
//
//        Assert.assertTrue(result.stream().findAny().isPresent)
//    }
//
//    fun testSpaces() {
//        val doc = (db.newEntity() as EntityImpl)
//        val test =
//            ("{"
//                    + "\"embedded\": {"
//                    + "\"second_embedded\":  {"
//                    + "\"text\":\"this is a test\""
//                    + "}"
//                    + "}"
//                    + "}")
//        doc.updateFromJSON(test)
//        Assert.assertTrue(doc.toJSON("fetchPlan:*:0,rid").contains("this is a test"))
//    }
//
//    fun testEscaping() {
//        val doc = (db.newEntity() as EntityImpl)
//        val s =
//            ("{\"name\": \"test\", \"nested\": { \"key\": \"value\", \"anotherKey\": 123 }, \"deep\":"
//                    + " {\"deeper\": { \"k\": \"v\",\"quotes\": \"\\\"\\\",\\\"oops\\\":\\\"123\\\"\","
//                    + " \"likeJson\": \"[1,2,3]\",\"spaces\": \"value with spaces\"}}}")
//        doc.updateFromJSON(s)
//        Assert.assertEquals(doc.field("deep[deeper][quotes]"), "\"\",\"oops\":\"123\"")
//
//        val res = doc.toJSON()
//
//        // LOOK FOR "quotes": \"\",\"oops\":\"123\"
//        Assert.assertTrue(res.contains("\"quotes\":\"\\\"\\\",\\\"oops\\\":\\\"123\\\"\""))
//    }
//
//    fun testEscapingDoubleQuotes() {
//        val doc = (db.newEntity() as EntityImpl)
//        val sb =
//            """
//             {
//                "foo":{
//                        "bar":{
//                            "P357":[
//                                        {
//
//                                            "datavalue":{
//                                                "value":"\${'"'}\${'"'}"
//                                            }
//                                    }
//                            ]
//                        },
//                        "three": "a"
//                    }
//            }
//            """.trimIndent()
//        doc.updateFromJSON(sb)
//        Assert.assertEquals(doc.field("foo.three"), "a")
//        val c = doc.field<Collection<*>>("foo.bar.P357")
//        Assert.assertEquals(c.size, 1)
//        val doc2 = c.iterator().next() as Map<*, *>
//        Assert.assertEquals((doc2["datavalue"] as Map<*, *>)["value"], "\"\"")
//    }
//
//    fun testEscapingDoubleQuotes2() {
//        val doc = (db.newEntity() as EntityImpl)
//        val sb =
//            """
//             {
//                "foo":{
//                        "bar":{
//                            "P357":[
//                                        {
//
//                                            "datavalue":{
//                                                "value":"\${'"'}",
//
//                                            }
//                                    }
//                            ]
//                        },
//                        "three": "a"
//                    }
//            }
//            """.trimIndent()
//
//        doc.updateFromJSON(sb)
//        Assert.assertEquals(doc.field("foo.three"), "a")
//        val c = doc.field<Collection<*>>("foo.bar.P357")
//        Assert.assertEquals(c.size, 1)
//        val doc2 = c.iterator().next() as Map<*, *>
//        Assert.assertEquals((doc2["datavalue"] as Map<*, *>)["value"], "\"")
//    }
//
//    fun testEscapingDoubleQuotes3() {
//        val doc = (db.newEntity() as EntityImpl)
//        val sb =
//            """
//             {
//                "foo":{
//                        "bar":{
//                            "P357":[
//                                        {
//
//                                            "datavalue":{
//                                                "value":"\${'"'}",
//
//                                            }
//                                    }
//                            ]
//                        }
//                    }
//            }
//            """.trimIndent()
//
//        doc.updateFromJSON(sb)
//        val c = doc.field<Collection<*>>("foo.bar.P357")
//        Assert.assertEquals(c.size, 1)
//        val doc2 = c.iterator().next() as Map<*, *>
//        Assert.assertEquals((doc2["datavalue"] as Map<*, *>)["value"], "\"")
//    }
//
//    fun testEmbeddedQuotes() {
//        val doc = (db.newEntity() as EntityImpl)
//        // FROM ISSUE 3151
//        doc.updateFromJSON("{\"mainsnak\":{\"datavalue\":{\"value\":\"Sub\\\\urban\"}}}")
//        Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Sub\\urban")
//    }
//
//    fun testEmbeddedQuotes2() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{\"datavalue\":{\"value\":\"Sub\\\\urban\"}}")
//        Assert.assertEquals(doc.field("datavalue.value"), "Sub\\urban")
//    }
//
//    fun testEmbeddedQuotes2a() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{\"datavalue\":\"Sub\\\\urban\"}")
//        Assert.assertEquals(doc.field("datavalue"), "Sub\\urban")
//    }
//
//    fun testEmbeddedQuotes3() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{\"mainsnak\":{\"datavalue\":{\"value\":\"Suburban\\\\\"\"}}}")
//        Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Suburban\\\"")
//    }
//
//    fun testEmbeddedQuotes4() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{\"datavalue\":{\"value\":\"Suburban\\\\\"\"}}")
//        Assert.assertEquals(doc.field("datavalue.value"), "Suburban\\\"")
//    }
//
//    fun testEmbeddedQuotes5() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{\"datavalue\":\"Suburban\\\\\"\"}")
//        Assert.assertEquals(doc.field("datavalue"), "Suburban\\\"")
//    }
//
//    fun testEmbeddedQuotes6() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{\"mainsnak\":{\"datavalue\":{\"value\":\"Suburban\\\\\"}}}")
//        Assert.assertEquals(doc.field("mainsnak.datavalue.value"), "Suburban\\")
//    }
//
//    fun testEmbeddedQuotes7() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{\"datavalue\":{\"value\":\"Suburban\\\\\"}}")
//        Assert.assertEquals(doc.field("datavalue.value"), "Suburban\\")
//    }
//
//    fun testEmbeddedQuotes8() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{\"datavalue\":\"Suburban\\\\\"}")
//        Assert.assertEquals(doc.field("datavalue"), "Suburban\\")
//    }
//
//    fun testEmpty() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{}")
//        Assert.assertEquals(doc.fieldNames().size, 0)
//    }
//
//    fun testInvalidJson() {
//        val doc = (db.newEntity() as EntityImpl)
//        try {
//            doc.updateFromJSON("{")
//            Assert.fail()
//        } catch (ignored: SerializationException) {
//        }
//
//        try {
//            doc.updateFromJSON("{\"foo\":{}")
//            Assert.fail()
//        } catch (ignored: SerializationException) {
//        }
//
//        try {
//            doc.updateFromJSON("{{}")
//            Assert.fail()
//        } catch (ignored: SerializationException) {
//        }
//
//        try {
//            doc.updateFromJSON("{}}")
//            Assert.fail()
//        } catch (ignored: SerializationException) {
//        }
//
//        try {
//            doc.updateFromJSON("}")
//            Assert.fail()
//        } catch (ignored: SerializationException) {
//        }
//    }
//
//    fun testDates() {
//        val now = Date(1350518475000L)
//
//        val doc = (db.newEntity() as EntityImpl)
//        doc.field("date", now)
//        val json = doc.toJSON()
//
//        val unmarshalled = (db.newEntity() as EntityImpl)
//        unmarshalled.updateFromJSON(json)
//        Assert.assertEquals(unmarshalled.field("date"), now)
//    }
//
//    @Test
//    fun shouldDeserializeFieldWithCurlyBraces() {
//        val json = "{\"a\":\"{dd}\",\"bl\":{\"b\":\"c\",\"a\":\"d\"}}"
//        val `in` =
//            RecordSerializerJackson.INSTANCE.fromString<EntityImpl>(
//                db,
//                json, db.newInstance(), arrayOf()
//            )
//        Assert.assertEquals(`in`.field("a"), "{dd}")
//        Assert.assertTrue(`in`.field<Any>("bl") is Map<*, *>)
//    }
//
//    @Test
//    @Throws(Exception::class)
//    fun testList() {
//        val documentSource = (db.newEntity() as EntityImpl)
//        documentSource.updateFromJSON("{\"list\" : [\"string\", 42]}")
//
//        val documentTarget = (db.newEntity() as EntityImpl)
//        RecordInternal.unsetDirty(documentTarget)
//        documentTarget.fromStream(documentSource.toStream())
//
//        val list = documentTarget.field<TrackedList<Any>>("list", PropertyType.EMBEDDEDLIST)
//        Assert.assertEquals(list[0], "string")
//        Assert.assertEquals(list[1], 42)
//    }
//
//    @Test
//    @Throws(Exception::class)
//    fun testEmbeddedRIDBagDeserialisationWhenFieldTypeIsProvided() {
//        val documentSource = (db.newEntity() as EntityImpl)
//        documentSource.updateFromJSON(
//            "{FirstName:\"Student A"
//                    + " 0\",in_EHasGoodStudents:[#57:0],@fieldTypes:\"in_EHasGoodStudents=g\"}"
//        )
//
//        val bag = documentSource.field<RidBag>("in_EHasGoodStudents")
//        Assert.assertEquals(bag.size(), 1)
//        val rid: Identifiable = bag.iterator().next()
//        Assert.assertEquals(rid.identity.clusterId, 57)
//        Assert.assertEquals(rid.identity.clusterPosition, 0)
//    }
//
//    fun testNestedLinkCreation() {
//        var jaimeDoc = (db.newEntity("NestedLinkCreation") as EntityImpl)
//        jaimeDoc.field("name", "jaime")
//
//        db.begin()
//        jaimeDoc.save()
//        db.commit()
//
//        jaimeDoc = db.bindToSession(jaimeDoc)
//        // The link between jaime and cersei is saved properly - the #2263 test case
//        val cerseiDoc = (db.newEntity("NestedLinkCreation") as EntityImpl)
//        cerseiDoc.updateFromJSON(
//            "{\"@type\":\"d\",\"name\":\"cersei\",\"valonqar\":" + jaimeDoc.toJSON() + "}"
//        )
//        db.begin()
//        cerseiDoc.save()
//        db.commit()
//
//        jaimeDoc = db.bindToSession(jaimeDoc)
//        // The link between jamie and tyrion is not saved properly
//        val tyrionDoc = (db.newEntity("NestedLinkCreation") as EntityImpl)
//        tyrionDoc.updateFromJSON(
//            ("{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":{\"@type\":\"d\","
//                    + " \"relationship\":\"brother\",\"contact\":"
//                    + jaimeDoc.toJSON()
//                    + "}}")
//        )
//
//        db.begin()
//        tyrionDoc.save()
//        db.commit()
//
//        val contentMap: MutableMap<RID, EntityImpl> = HashMap()
//
//        val jaime = (db.newEntity("NestedLinkCreation") as EntityImpl)
//        jaime.field("name", "jaime")
//
//        contentMap[jaimeDoc.identity] = jaime
//
//        val cersei = (db.newEntity("NestedLinkCreation") as EntityImpl)
//        cersei.field("name", "cersei")
//        cersei.field("valonqar", jaimeDoc.identity)
//        contentMap[cerseiDoc.identity] = cersei
//
//        val tyrion = (db.newEntity("NestedLinkCreation") as EntityImpl)
//        tyrion.field("name", "tyrion")
//
//        val embeddedDoc = (db.newEntity() as EntityImpl)
//        embeddedDoc.field("relationship", "brother")
//        embeddedDoc.field("contact", jaimeDoc.identity)
//        tyrion.field("emergency_contact", embeddedDoc)
//
//        contentMap[tyrionDoc.identity] = tyrion
//
//        val traverseMap: MutableMap<RID, MutableList<RID>> = HashMap()
//        val jaimeTraverse: MutableList<RID> = ArrayList()
//        jaimeTraverse.add(jaimeDoc.identity)
//        traverseMap[jaimeDoc.identity] = jaimeTraverse
//
//        val cerseiTraverse: MutableList<RID> = ArrayList()
//        cerseiTraverse.add(cerseiDoc.identity)
//        cerseiTraverse.add(jaimeDoc.identity)
//
//        traverseMap[cerseiDoc.identity] = cerseiTraverse
//
//        val tyrionTraverse: MutableList<RID> = ArrayList()
//        tyrionTraverse.add(tyrionDoc.identity)
//        tyrionTraverse.add(jaimeDoc.identity)
//        traverseMap[tyrionDoc.identity] = tyrionTraverse
//
//        for (o in db.browseClass("NestedLinkCreation")) {
//            val content = contentMap[o.identity]
//            Assert.assertTrue(content!!.hasSameContentOf(o))
//
//            val traverse =
//                traverseMap.remove(o.identity)!!
//            for (id in SQLSynchQuery<EntityImpl>("traverse * from " + o.identity.toString())) {
//                Assert.assertTrue(traverse.remove(id.identity))
//            }
//
//            Assert.assertTrue(traverse.isEmpty())
//        }
//
//        Assert.assertTrue(traverseMap.isEmpty())
//    }
//
//    fun testNestedLinkCreationFieldTypes() {
//        val jaimeDoc = (db.newEntity("NestedLinkCreationFieldTypes") as EntityImpl)
//        jaimeDoc.field("name", "jaime")
//
//        db.begin()
//        jaimeDoc.save()
//        db.commit()
//
//        // The link between jaime and cersei is saved properly - the #2263 test case
//        val cerseiDoc = (db.newEntity("NestedLinkCreationFieldTypes") as EntityImpl)
//        cerseiDoc.updateFromJSON(
//            ("{\"@type\":\"d\",\"@fieldTypes\":\"valonqar=x\",\"name\":\"cersei\",\"valonqar\":"
//                    + jaimeDoc.identity
//                    + "}")
//        )
//
//        db.begin()
//        cerseiDoc.save()
//        db.commit()
//
//        // The link between jamie and tyrion is not saved properly
//        val tyrionDoc = (db.newEntity("NestedLinkCreationFieldTypes") as EntityImpl)
//        tyrionDoc.updateFromJSON(
//            ("{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":{\"@type\":\"d\","
//                    + " \"@fieldTypes\":\"contact=x\",\"relationship\":\"brother\",\"contact\":"
//                    + jaimeDoc.identity
//                    + "}}")
//        )
//
//        db.begin()
//        tyrionDoc.save()
//        db.commit()
//
//        val contentMap: MutableMap<RID, EntityImpl> = HashMap()
//
//        val jaime = (db.newEntity("NestedLinkCreationFieldTypes") as EntityImpl)
//        jaime.field("name", "jaime")
//
//        contentMap[jaimeDoc.identity] = jaime
//
//        val cersei = (db.newEntity("NestedLinkCreationFieldTypes") as EntityImpl)
//        cersei.field("name", "cersei")
//        cersei.field("valonqar", jaimeDoc.identity)
//        contentMap[cerseiDoc.identity] = cersei
//
//        val tyrion = (db.newEntity("NestedLinkCreationFieldTypes") as EntityImpl)
//        tyrion.field("name", "tyrion")
//
//        val embeddedDoc = (db.newEntity() as EntityImpl)
//        embeddedDoc.field("relationship", "brother")
//        embeddedDoc.field("contact", jaimeDoc.identity)
//        tyrion.field("emergency_contact", embeddedDoc)
//
//        contentMap[tyrionDoc.identity] = tyrion
//
//        val traverseMap: MutableMap<RID, MutableList<RID>> = HashMap()
//        val jaimeTraverse: MutableList<RID> = ArrayList()
//        jaimeTraverse.add(jaimeDoc.identity)
//        traverseMap[jaimeDoc.identity] = jaimeTraverse
//
//        val cerseiTraverse: MutableList<RID> = ArrayList()
//        cerseiTraverse.add(cerseiDoc.identity)
//        cerseiTraverse.add(jaimeDoc.identity)
//
//        traverseMap[cerseiDoc.identity] = cerseiTraverse
//
//        val tyrionTraverse: MutableList<RID> = ArrayList()
//        tyrionTraverse.add(tyrionDoc.identity)
//        tyrionTraverse.add(jaimeDoc.identity)
//        traverseMap[tyrionDoc.identity] = tyrionTraverse
//
//        for (o in db.browseClass("NestedLinkCreationFieldTypes")) {
//            val content = contentMap[o.identity]
//            Assert.assertTrue(content!!.hasSameContentOf(o))
//
//            val traverse =
//                traverseMap.remove(o.identity)!!
//            for (id in SQLSynchQuery<EntityImpl>("traverse * from " + o.identity.toString())) {
//                Assert.assertTrue(traverse.remove(id.identity))
//            }
//            Assert.assertTrue(traverse.isEmpty())
//        }
//        Assert.assertTrue(traverseMap.isEmpty())
//    }
//
//    fun testInnerDocCreation() {
//        var adamDoc = (db.newEntity("InnerDocCreation") as EntityImpl)
//        adamDoc.updateFromJSON("{\"name\":\"adam\"}")
//
//        db.begin()
//        adamDoc.save()
//        db.commit()
//
//        db.begin()
//        adamDoc = db.bindToSession(adamDoc)
//        val eveDoc = (db.newEntity("InnerDocCreation") as EntityImpl)
//        eveDoc.updateFromJSON(
//            "{\"@type\":\"d\",\"name\":\"eve\",\"friends\":[" + adamDoc.toJSON() + "]}"
//        )
//
//        eveDoc.save()
//        db.commit()
//
//        val contentMap: MutableMap<RID, EntityImpl> = HashMap()
//        val adam = (db.newEntity("InnerDocCreation") as EntityImpl)
//        adam.field("name", "adam")
//
//        contentMap[adamDoc.identity] = adam
//
//        val eve = (db.newEntity("InnerDocCreation") as EntityImpl)
//        eve.field("name", "eve")
//
//        val friends: MutableList<RID> = ArrayList()
//        friends.add(adamDoc.identity)
//        eve.field("friends", friends)
//
//        contentMap[eveDoc.identity] = eve
//
//        val traverseMap: MutableMap<RID, MutableList<RID>> = HashMap()
//
//        val adamTraverse: MutableList<RID> = ArrayList()
//        adamTraverse.add(adamDoc.identity)
//        traverseMap[adamDoc.identity] = adamTraverse
//
//        val eveTraverse: MutableList<RID> = ArrayList()
//        eveTraverse.add(eveDoc.identity)
//        eveTraverse.add(adamDoc.identity)
//
//        traverseMap[eveDoc.identity] = eveTraverse
//
//        for (o in db.browseClass("InnerDocCreation")) {
//            val content = contentMap[o.identity]
//            Assert.assertTrue(content!!.hasSameContentOf(o))
//        }
//
//        for (o in db.browseClass("InnerDocCreation")) {
//            val traverse =
//                traverseMap.remove(o.identity)!!
//            for (id in SQLSynchQuery<EntityImpl>("traverse * from " + o.identity.toString())) {
//                Assert.assertTrue(traverse.remove(id.identity))
//            }
//            Assert.assertTrue(traverse.isEmpty())
//        }
//        Assert.assertTrue(traverseMap.isEmpty())
//    }
//
//    fun testInnerDocCreationFieldTypes() {
//        val adamDoc = (db.newEntity("InnerDocCreationFieldTypes") as EntityImpl)
//        adamDoc.updateFromJSON("{\"name\":\"adam\"}")
//
//        db.begin()
//        adamDoc.save()
//        db.commit()
//
//        val eveDoc = (db.newEntity("InnerDocCreationFieldTypes") as EntityImpl)
//        eveDoc.updateFromJSON(
//            ("{\"@type\":\"d\", \"@fieldTypes\" : \"friends=z\", \"name\":\"eve\",\"friends\":["
//                    + adamDoc.identity
//                    + "]}")
//        )
//
//        db.begin()
//        eveDoc.save()
//        db.commit()
//
//        val contentMap: MutableMap<RID, EntityImpl> = HashMap()
//        val adam = (db.newEntity("InnerDocCreationFieldTypes") as EntityImpl)
//        adam.field("name", "adam")
//
//        contentMap[adamDoc.identity] = adam
//
//        val eve = (db.newEntity("InnerDocCreationFieldTypes") as EntityImpl)
//        eve.field("name", "eve")
//
//        val friends: MutableList<RID> = ArrayList()
//        friends.add(adamDoc.identity)
//        eve.field("friends", friends)
//
//        contentMap[eveDoc.identity] = eve
//
//        val traverseMap: MutableMap<RID, MutableList<RID>> = HashMap()
//
//        val adamTraverse: MutableList<RID> = ArrayList()
//        adamTraverse.add(adamDoc.identity)
//        traverseMap[adamDoc.identity] = adamTraverse
//
//        val eveTraverse: MutableList<RID> = ArrayList()
//        eveTraverse.add(eveDoc.identity)
//        eveTraverse.add(adamDoc.identity)
//
//        traverseMap[eveDoc.identity] = eveTraverse
//
//        for (o in db.browseClass("InnerDocCreationFieldTypes")) {
//            val content = contentMap[o.identity]
//            Assert.assertTrue(content!!.hasSameContentOf(o))
//        }
//
//        for (o in db.browseClass("InnerDocCreationFieldTypes")) {
//            val traverse =
//                traverseMap.remove(o.identity)!!
//            for (id in SQLSynchQuery<EntityImpl>("traverse * from " + o.identity.toString())) {
//                Assert.assertTrue(traverse.remove(id.identity))
//            }
//
//            Assert.assertTrue(traverse.isEmpty())
//        }
//
//        Assert.assertTrue(traverseMap.isEmpty())
//    }
//
//    fun testJSONTxDocInsertOnly() {
//        val classNameDocOne = "JSONTxDocOneInsertOnly"
//        if (!db.metadata.schema.existsClass(classNameDocOne)) {
//            db.metadata.schema.createClass(classNameDocOne)
//        }
//        val classNameDocTwo = "JSONTxDocTwoInsertOnly"
//        if (!db.metadata.schema.existsClass(classNameDocTwo)) {
//            db.metadata.schema.createClass(classNameDocTwo)
//        }
//        db.begin()
//        val eveDoc = (db.newEntity(classNameDocOne) as EntityImpl)
//        eveDoc.field("name", "eve")
//        eveDoc.save()
//
//        val nestedWithTypeD = (db.newEntity(classNameDocTwo) as EntityImpl)
//        nestedWithTypeD.updateFromJSON(
//            "{\"@type\":\"d\",\"event_name\":\"world cup 2014\",\"admin\":[" + eveDoc.toJSON() + "]}"
//        )
//        nestedWithTypeD.save()
//        db.commit()
//        Assert.assertEquals(db.countClass(classNameDocOne), 1)
//
//        val contentMap: MutableMap<RID, EntityImpl> = HashMap()
//        val eve = (db.newEntity(classNameDocOne) as EntityImpl)
//        eve.field("name", "eve")
//        contentMap[eveDoc.identity] = eve
//
//        for (document in db.browseClass(classNameDocOne)) {
//            val content = contentMap[document.identity]
//            Assert.assertTrue(content!!.hasSameContentOf(document))
//        }
//    }
//
//    fun testJSONTxDoc() {
//        if (!db.metadata.schema.existsClass("JSONTxDocOne")) {
//            db.metadata.schema.createClass("JSONTxDocOne")
//        }
//
//        if (!db.metadata.schema.existsClass("JSONTxDocTwo")) {
//            db.metadata.schema.createClass("JSONTxDocTwo")
//        }
//
//        var adamDoc = (db.newEntity("JSONTxDocOne") as EntityImpl)
//        adamDoc.field("name", "adam")
//
//        db.begin()
//        adamDoc.save()
//        db.commit()
//
//        db.begin()
//        val eveDoc = (db.newEntity("JSONTxDocOne") as EntityImpl)
//        eveDoc.field("name", "eve")
//        eveDoc.save()
//
//        adamDoc = db.bindToSession(adamDoc)
//        val nestedWithTypeD = (db.newEntity("JSONTxDocTwo") as EntityImpl)
//        nestedWithTypeD.updateFromJSON(
//            ("{\"@type\":\"d\",\"event_name\":\"world cup 2014\",\"admin\":["
//                    + eveDoc.toJSON()
//                    + ","
//                    + adamDoc.toJSON()
//                    + "]}")
//        )
//        nestedWithTypeD.save()
//
//        db.commit()
//
//        Assert.assertEquals(db.countClass("JSONTxDocOne"), 2)
//
//        val contentMap: MutableMap<RID, EntityImpl> = HashMap()
//        val adam = (db.newEntity("JSONTxDocOne") as EntityImpl)
//        adam.field("name", "adam")
//        contentMap[adamDoc.identity] = adam
//
//        val eve = (db.newEntity("JSONTxDocOne") as EntityImpl)
//        eve.field("name", "eve")
//        contentMap[eveDoc.identity] = eve
//
//        for (o in db.browseClass("JSONTxDocOne")) {
//            val content = contentMap[o.identity]
//            Assert.assertTrue(content!!.hasSameContentOf(o))
//        }
//    }
//
//    fun testInvalidLink() {
//        val nullRefDoc = (db.newEntity() as EntityImpl)
//        nullRefDoc.updateFromJSON("{\"name\":\"Luca\", \"ref\":\"#-1:-1\"}")
//
//        // Assert.assertNull(nullRefDoc.rawField("ref"));
//        val json = nullRefDoc.toJSON()
//        val pos = json.indexOf("\"ref\":")
//
//        Assert.assertTrue(pos > -1)
//        Assert.assertEquals(json[pos + "\"ref\":".length], 'n')
//    }
//
//    fun testOtherJson() {
//        db.newEntity()
//            .updateFromJSON(
//                ("{\"Salary\":1500.0,\"Type\":\"Person\",\"Address\":[{\"Zip\":\"JX2"
//                        + " MSX\",\"Type\":\"Home\",\"Street1\":\"13 Marge"
//                        + " Street\",\"Country\":\"Holland\",\"Id\":\"Address-28813211\",\"City\":\"Amsterdam\",\"From\":\"1996-02-01\",\"To\":\"1998-01-01\"},{\"Zip\":\"90210\",\"Type\":\"Work\",\"Street1\":\"100"
//                        + " Hollywood"
//                        + " Drive\",\"Country\":\"USA\",\"Id\":\"Address-11595040\",\"City\":\"Los"
//                        + " Angeles\",\"From\":\"2009-09-01\"}],\"Id\":\"Person-7464251\",\"Name\":\"Stan\"}")
//            )
//    }
//
//    @Test
//    fun testScientificNotation() {
//        val doc = (db.newEntity() as EntityImpl)
//        doc.updateFromJSON("{'number1': -9.2741500e-31, 'number2': 741800E+290}")
//
//        val number1 = doc.field<Double>("number1")
//        Assert.assertEquals(number1, -9.27415E-31)
//        val number2 = doc.field<Double>("number2")
//        Assert.assertEquals(number2, 741800E+290)
//    }

    companion object {
        const val FORMAT_WITHOUT_RID: String = "version,class,type"
    }
}
