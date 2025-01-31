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

import com.jetbrains.youtrack.db.api.exception.CommitSerializationException
import com.jetbrains.youtrack.db.api.record.Entity
import com.jetbrains.youtrack.db.api.record.RID
import com.jetbrains.youtrack.db.api.schema.PropertyType
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException
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

        db.createClass("Device")
        db.createClass("Track")
        db.createClass("NestedLinkCreation")
        db.createClass("NestedLinkCreationFieldTypes")
        db.createClass("InnerDocCreation")
        db.createClass("InnerDocCreationFieldTypes")
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
    fun testNumericDoubleList() {
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
            Assert.assertEquals(list[0], 17.3)
            Assert.assertEquals(list[1], 42.7)
        }
    }

    @Test
    fun testNullity() {
        val rid = db.computeInTx {
            val entity = db.newEntity()
            entity.updateFromJSON(
                "{\"gender\":{\"name\":\"Male\"},\"firstName\":\"Jack\",\"lastName\":\"Williams\"," +
                        "\"phone\":\"561-401-3348\",\"email\":\"0586548571@example.com\",\"address\":{\"street1\":\"Smith" +
                        " Ave\"," +
                        "\"street2\":null,\"city\":\"GORDONSVILLE\",\"state\":\"VA\",\"code\":\"22942\"},\"dob\":\"2011-11-17" +
                        " 03:17:04\"}"
            )
            entity.identity
        }

        checkJsonSerialization(rid)
        val expectedMap = mapOf(
            "gender" to mapOf(
                "name" to "Male"
            ),
            "firstName" to "Jack",
            "lastName" to "Williams",
            "phone" to "561-401-3348",
            "email" to "0586548571@example.com",
            "address" to mapOf(
                "street1" to "Smith Ave",
                "street2" to null,
                "city" to "GORDONSVILLE",
                "state" to "VA",
                "code" to "22942"
            ),
            "dob" to "2011-11-17 03:17:04",
            "@rid" to rid,
            "@class" to "O"
        )
        checkJsonSerialization(rid, expectedMap)
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

        checkJsonSerialization(rid)
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

        checkJsonSerialization(rid)
    }

    @Test
    fun testListToJSON() {
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


        checkJsonSerialization(rid)
    }

    @Test
    fun testEmptyEmbeddedMap() {
        val rid = db.computeInTx {
            val original = db.newEntity()
            original.getOrCreateEmbeddedMap<Entity>("embeddedMap")
            original.identity
        }
        checkJsonSerialization(rid)
    }

    @Test
    fun testMultiLevelTypes() {
        val rid = db.computeInTx {
            val newEntity = db.newEntity()
            newEntity.setProperty("long", 100000000000L)
            newEntity.setProperty("date", Date())
            newEntity.setProperty("byte", 12.toByte())

            val firstLevelEntity = db.newEntity()
            firstLevelEntity.setProperty("long", 200000000000L)
            firstLevelEntity.setProperty("date", Date())
            firstLevelEntity.setProperty("byte", 13.toByte())

            val secondLevelEntity = db.newEntity()
            secondLevelEntity.setProperty("long", 300000000000L)
            secondLevelEntity.setProperty("date", Date())
            secondLevelEntity.setProperty("byte", 14.toByte())

            val thirdLevelEntity = db.newEntity()
            thirdLevelEntity.setProperty("long", 400000000000L)
            thirdLevelEntity.setProperty("date", Date())
            thirdLevelEntity.setProperty("byte", 15.toByte())

            newEntity.setProperty("doc", firstLevelEntity)
            firstLevelEntity.setProperty("doc", secondLevelEntity)
            secondLevelEntity.setProperty("doc", thirdLevelEntity)

            newEntity.identity
        }

        checkJsonSerialization(rid)
    }

    @Test
    fun testNestedEmbeddedMap() {
        val rid = db.computeInTx {
            val entity = db.newEntity()

            val map1 = mutableMapOf<String, Map<String, Any>>()
            entity.getOrCreateEmbeddedMap<Any>("map1")["map1"] = map1

            val map2 = mutableMapOf<String, Map<String, Any>>()
            map1["map2"] = map2

            val map3 = mutableMapOf<String, Map<String, Any>>()
            map2["map3"] = map3

            entity.identity
        }

        checkJsonSerialization(rid)
    }

    @Test
    fun testFetchedJson() {
        val resultSet =
            db
                .command("select * from Profile where name = 'Barack' and surname = 'Obama'")
                .toList()

        for (result in resultSet) {
            val entity = result.asEntity()!!
            checkJsonSerialization(entity.identity)
        }
    }

    @Test
    fun testSpecialChar() {
        val rid = db.computeInTx {
            val entity =
                db.entityFromJson("{\"name\":{\"%Field\":[\"value1\",\"value2\"],\"%Field2\":{},\"%Field3\":\"value3\"}}")
            entity.identity
        }

        checkJsonSerialization(rid)

        val expectedMap = mapOf(
            "name" to mapOf(
                "%Field" to listOf("value1", "value2"),
                "%Field2" to mapOf<String, Any>(),
                "%Field3" to "value3"
            ),
            "@rid" to rid,
            "@class" to "O"
        )
        checkJsonSerialization(rid, expectedMap)
    }

    @Test
    fun testArrayOfArray() {
        val rid = db.computeInTx {
            val entity = db.entityFromJson(
                "{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ [ 100,"
                        + "  0 ],  [ 101, 1 ] ]}"
            )
            entity.identity
        }

        checkJsonSerialization(rid)
        val expectedMap = mapOf(
            "@class" to "Track",
            "type" to "LineString",
            "coordinates" to listOf(
                listOf(100, 0),
                listOf(101, 1)
            ),
            "@rid" to rid
        )
        checkJsonSerialization(rid, expectedMap)
    }

    @Test
    fun testLongTypes() {
        val rid = db.computeInTx {
            val entity = db.entityFromJson(
                "{\"@type\": \"d\",\"@class\": \"Track\",\"type\": \"LineString\",\"coordinates\": [ ["
                        + " 32874387347347,  0 ],  [ -23736753287327, 1 ] ]}"
            )
            entity.identity
        }

        checkJsonSerialization(rid)
        val expectedMap = mapOf(
            "@class" to "Track",
            "type" to "LineString",
            "coordinates" to listOf(
                listOf<Any>(32874387347347L, 0),
                listOf<Any>(-23736753287327L, 1)
            ),
            "@rid" to rid
        )
        checkJsonSerialization(rid, expectedMap)
    }

    @Test
    fun testSpecialChars() {
        val rid = db.computeInTx {
            val entity =
                db.entityFromJson("{\"Field\":{\"Key1\":[\"Value1\",\"Value2\"],\"Key2\":{\"%%dummy%%\":null},\"Key3\":\"Value3\"}}")
            entity.identity

        }

        checkJsonSerialization(rid)

        val expectedMap = mapOf(
            "Field" to mapOf(
                "Key1" to listOf("Value1", "Value2"),
                "Key2" to mapOf("%%dummy%%" to null),
                "Key3" to "Value3"
            ),
            "@rid" to rid,
            "@class" to "O"
        )

        checkJsonSerialization(rid, expectedMap)
    }


    @Test
    fun testSameNameCollectionsAndMap() {
        val rid1 = db.computeInTx {
            val entity = db.newEntity()
            entity.setProperty("string", "STRING_VALUE")
            for (i in 0..0) {
                val entity1 = db.newEntity()
                entity.setProperty("number", i)
                entity.getOrCreateLinkSet("out").add(entity1)

                for (j in 0..0) {
                    val entity2 = db.newEntity()
                    entity2.setProperty("blabla", j)
                    entity1.getOrCreateLinkMap("out")[j.toString()] = entity2
                    val doc3 = (db.newEntity() as EntityImpl)
                    doc3.field("blubli", 0.toString())
                    entity2.setProperty("out", doc3)
                }
            }

            entity.identity
        }

        checkJsonSerialization(rid1)

        val rid2 = db.computeInTx {
            val entity = db.newEntity()
            entity.setProperty("string", "STRING_VALUE")
            for (i in 0..9) {
                val entity1 = db.newEntity()
                entity.setProperty("number", i)

                entity1.getOrCreateLinkList("out").add(entity)
                for (j in 0..4) {
                    val entity2 = db.newEntity()
                    entity2.setProperty("blabla", j)

                    entity1.getOrCreateLinkList("out").add(entity2)
                    val entity3 = db.newEntity()

                    entity3.setProperty("blubli", (i + j).toString())
                    entity2.setProperty("out", entity3)
                }
                entity.getOrCreateLinkMap("out")[i.toString()] = entity1
            }
            entity.identity
        }

        checkJsonSerialization(rid2)
    }

    @Test
    fun testSameNameCollectionsAndMap2() {
        val rid = db.computeInTx {
            val entity = db.newEntity()
            entity.setProperty("string", "STRING_VALUE")

            for (i in 0..1) {
                val entity1 = db.newEntity()
                entity.getOrCreateLinkList("theList").add(entity1)

                for (j in 0..4) {
                    val entity2 = db.newEntity()
                    entity2.setProperty("blabla", j)
                    entity1.getOrCreateLinkMap("theMap")[j.toString()] = entity2
                }

                entity.getOrCreateLinkList("theList").add(entity1)
            }

            entity.identity
        }

        checkJsonSerialization(rid)
    }

    @Test
    fun testSameNameCollectionsAndMap3() {
        val rid = db.computeInTx {
            val entity = db.newEntity()
            entity.setProperty("string", "STRING_VALUE")
            for (i in 0..1) {
                val docMap = mutableMapOf<String, Entity>()

                for (j in 0..4) {
                    val doc1 = (db.newEntity() as EntityImpl)
                    doc1.field("blabla", j)
                    docMap[j.toString()] = doc1
                }

                entity.getOrCreateEmbeddedList<Map<String, Entity>>("theList").add(docMap)
            }
            entity.identity
        }

        checkJsonSerialization(rid)
    }

    @Test
    fun testNestedJsonCollection() {
        db.executeInTx {
            db
                .command(
                    "insert into device (resource_id, domainset) VALUES (0, [ { 'domain' : 'abc' }, {"
                            + " 'domain' : 'pqr' } ])"
                )
                .close()
        }

        var result = db.query("select from device where domainset.domain contains 'abc'")
        Assert.assertTrue(result.stream().findAny().isPresent)

        result = db.query("select from device where domainset[domain = 'abc'] is not null")
        Assert.assertTrue(result.stream().findAny().isPresent)

        result = db.query("select from device where domainset.domain contains 'pqr'")
        Assert.assertTrue(result.stream().findAny().isPresent)
    }

    @Test
    fun testNestedEmbeddedJson() {
        db.executeInTx {
            db
                .command("insert into device (resource_id, domainset) VALUES (1, { 'domain' : 'eee' })")
                .close()
        }

        db.executeInTx {
            val result = db.query("select from device where domainset.domain = 'eee'")
            Assert.assertTrue(result.stream().findAny().isPresent)
        }
    }

    @Test
    fun testNestedMultiLevelEmbeddedJson() {
        db.executeInTx {
            db
                .command(
                    "insert into device (domainset) values ({'domain' : { 'lvlone' : { 'value' : 'five' } }"
                            + " } )"
                )
                .close()
        }

        db.executeInTx {
            val result =
                db.query("select from device where domainset.domain.lvlone.value = 'five'")

            Assert.assertTrue(result.stream().findAny().isPresent)
        }
    }

    @Test
    fun testSpaces() {
        val rid = db.computeInTx {
            val entity = db.newEntity()
            val test =
                ("{"
                        + "\"embedded\": {"
                        + "\"second_embedded\":  {"
                        + "\"text\":\"this is a test\""
                        + "}"
                        + "}"
                        + "}")
            entity.updateFromJSON(test)
            entity.identity
        }

        db.executeInTx {
            val entity = db.loadEntity(rid)
            Assert.assertTrue(entity.toJSON("fetchPlan:*:0,rid").contains("this is a test"))
        }
    }

    @Test
    fun testEscaping() {
        val rid = db.computeInTx {
            val s =
                ("{\"name\": \"test\", \"nested\": { \"key\": \"value\", \"anotherKey\": 123 }, \"deep\":"
                        + " {\"deeper\": { \"k\": \"v\",\"quotes\": \"\\\"\\\",\\\"oops\\\":\\\"123\\\"\","
                        + " \"likeJson\": \"[1,2,3]\",\"spaces\": \"value with spaces\"}}}")
            val entity = db.entityFromJson(s)
            Assert.assertEquals(
                entity.getProperty<Map<String, Map<String, String>>>("deep")!!["deeper"]!!["quotes"],
                "\"\",\"oops\":\"123\""
            )
            entity.identity
        }

        db.executeInTx {
            val res = db.loadEntity(rid).toJSON()
            Assert.assertTrue(res.contains("\"quotes\":\"\\\"\\\",\\\"oops\\\":\\\"123\\\"\""))
        }
    }

    @Test
    fun testEscapingDoubleQuotes() {
        val rid = db.computeInTx {
            val sb =
                (""" {
                        "foo":{
                                "bar":{
                                    "P357":[
                                                {
                    
                                                    "datavalue":{
                                                        "value":"\"\"" 
                                                    }
                                            }
                                    ]   
                                },
                                "three": "a"
                            }
                    } """)
            db.entityFromJson(sb).identity
        }

        db.executeInTx {
            val entity = db.loadEntity(rid)
            Assert.assertEquals(entity.getProperty<Map<String, Any>>("foo")!!["three"], "a")

            val c =
                entity.getProperty<Map<String, Map<String, List<Map<String, Map<String, String>>>>>>(
                    "foo"
                )!!["bar"]!!["P357"]!!
            Assert.assertEquals(c.size, 1)
            val map = c.iterator().next()
            Assert.assertEquals((map["datavalue"] as Map<*, *>)["value"], "\"\"")
        }
    }

    @Test
    fun testEscapingDoubleQuotes2() {
        val rid = db.computeInTx {
            val sb =
                (""" {
                        "foo":{
                                "bar":{
                                    "P357":[
                                                {
                    
                                                    "datavalue":{
                                                        "value":"\""
                    
                                                    }
                                            }
                                    ]   
                                },
                                "three": "a"
                        }
                    } """)
            db.entityFromJson(sb).identity
        }

        db.executeInTx {
            val entity = db.loadEntity(rid)
            Assert.assertEquals(entity.getProperty<Map<String, Any>>("foo")!!["three"], "a")

            val c =
                entity.getProperty<Map<String, Map<String, List<Map<String, Map<String, String>>>>>>(
                    "foo"
                )!!["bar"]!!["P357"]!!
            Assert.assertEquals(c.size, 1)
            val map = c.iterator().next()
            Assert.assertEquals((map["datavalue"] as Map<*, *>)["value"], "\"")
        }
    }

    @Test
    fun testEscapingDoubleQuotes3() {
        val rid = db.computeInTx {
            val sb =
                (""" {
                        "foo":{
                                "bar":{
                                    "P357":[
                                                {
                    
                                                    "datavalue":{
                                                        "value":"\""
                    
                                                    }
                                            }
                                    ]   
                                }
                            }
                     } """)
            db.entityFromJson(sb).identity
        }

        db.executeInTx {
            val entity = db.loadEntity(rid)
            val c =
                entity.getProperty<Map<String, Map<String, List<Map<String, Map<String, String>>>>>>(
                    "foo"
                )!!["bar"]!!["P357"]!!
            Assert.assertEquals(c.size, 1)
            val map = c.iterator().next()
            Assert.assertEquals((map["datavalue"] as Map<*, *>)["value"], "\"")
        }
    }

    @Test
    fun testEmbeddedQuotes() {
        db.executeInTx {
            val entity =
                db.entityFromJson("{\"mainsnak\":{\"datavalue\":{\"value\":\"Sub\\\\urban\"}}}")
            Assert.assertEquals(
                entity.getProperty<Map<String, Map<String, String>>>("mainsnak")!!["datavalue"]!!["value"],
                "Sub\\urban"
            )
        }
    }

    @Test
    fun testEmbeddedQuotes2() {
        db.executeInTx {
            val entity = db.entityFromJson("{\"datavalue\":{\"value\":\"Sub\\\\urban\"}}")
            Assert.assertEquals(
                entity.getProperty<Map<String, Map<String, String>>>("datavalue")!!["value"],
                "Sub\\urban"
            )
        }
    }

    @Test
    fun testEmbeddedQuotes2a() {
        db.executeInTx {
            val entity = db.entityFromJson("{\"datavalue\":\"Sub\\\\urban\"}")
            Assert.assertEquals(entity.getProperty("datavalue"), "Sub\\urban")
        }
    }

    @Test
    fun testEmbeddedQuotes3() {
        db.executeInTx {
            val entity =
                db.entityFromJson("{\"mainsnak\":{\"datavalue\":{\"value\":\"Suburban\\\\\\\"\"}}}")
            Assert.assertEquals(
                entity.getProperty<Map<String, Map<String, String>>>("mainsnak")!!["datavalue"]!!["value"],
                "Suburban\\\""
            )
        }
    }

    @Test
    fun testEmbeddedQuotes4() {
        db.executeInTx {
            val entity = db.entityFromJson("{\"datavalue\":\"Suburban\\\\\\\"\"}")
            Assert.assertEquals(entity.getProperty("datavalue"), "Suburban\\\"")
        }
    }

    @Test
    fun testEmbeddedQuotes5() {
        db.executeInTx {
            val entity = db.newEntity()
            entity.updateFromJSON("{\"mainsnak\":{\"datavalue\":{\"value\":\"Suburban\\\\\"}}}")
            Assert.assertEquals(
                entity.getProperty<Map<String, Map<String, String>>>("mainsnak")!!["datavalue"]!!["value"],
                "Suburban\\"
            )
        }
    }

    @Test
    fun testEmbeddedQuotes6() {
        db.executeInTx {
            val entity = db.newEntity()
            entity.updateFromJSON("{\"datavalue\":{\"value\":\"Suburban\\\\\"}}")
            Assert.assertEquals(
                entity.getProperty<Map<String, Map<String, String>>>("datavalue")!!["value"],
                "Suburban\\"
            )
        }
    }

    @Test
    fun testEmbeddedQuotes7() {
        db.executeInTx {
            val entity = db.entityFromJson("{\"datavalue\":\"Suburban\\\\\"}")
            Assert.assertEquals(entity.getProperty("datavalue"), "Suburban\\")
        }
    }


    @Test
    fun testEmpty() {
        db.executeInTx {
            val entity = db.entityFromJson("{}")
            Assert.assertTrue(entity.propertyNames.isEmpty())
        }
    }

    @Test
    fun testInvalidJson() {
        try {
            db.executeInTx {
                db.entityFromJson("{")
            }
            Assert.fail()
        } catch (ignored: SerializationException) {
        }


        try {
            db.executeInTx {
                db.entityFromJson("{\"foo\":{}")
            }
            Assert.fail()
        } catch (ignored: SerializationException) {
        }

        try {
            db.executeInTx {
                db.entityFromJson("{{}")
            }
        } catch (ignored: SerializationException) {
        }

        try {
            db.executeInTx {
                db.entityFromJson("{}}")
            }
            Assert.fail()
        } catch (ignored: SerializationException) {
        }

        try {
            db.executeInTx {
                db.entityFromJson("}")
            }
            Assert.fail()
        } catch (ignored: SerializationException) {
        }
    }

    @Test
    fun testDates() {
        val now = Date(1350518475000L)

        val rid = db.computeInTx {
            val entity = db.newEntity()
            entity.setProperty("date", now)
            entity.identity
        }

        db.executeInTx {
            val entity = db.loadEntity(rid)
            val json = entity.toJSON(FORMAT_WITHOUT_RID)

            val unmarshalled = db.entityFromJson(json)
            Assert.assertEquals(unmarshalled.getProperty("date"), now)
        }
    }

    @Test
    fun shouldDeserializeFieldWithCurlyBraces() {
        val json = "{\"a\":\"{dd}\",\"bl\":{\"b\":\"c\",\"a\":\"d\"}}"
        db.executeInTx {
            val entity = db.entityFromJson(json)
            Assert.assertEquals(entity.getProperty("a"), "{dd}")
            Assert.assertTrue(entity.getProperty<Any>("bl") is Map<*, *>)
        }
    }

    @Test
    fun testList() {
        db.executeInTx {
            val entity = db.entityFromJson("{\"list\" : [\"string\", 42]}")
            val list = entity.getProperty<List<Any>>("list")

            Assert.assertEquals(list!![0], "string")
            Assert.assertEquals(list[1], 42)
        }
    }

    @Test
    fun testEmbeddedRIDBagDeserialisationWhenFieldTypeIsProvided() {
        db.executeInTx {
            val entity = db.entityFromJson(
                "{\"@fieldTypes\":{\"in_EHasGoodStudents\": \"g\"}, \"FirstName\":\"Student A"
                        + " 0\",\"in_EHasGoodStudents\":[\"#57:0\"]}"
            )

            val bag = entity.getProperty<RidBag>("in_EHasGoodStudents")!!
            Assert.assertEquals(bag.size(), 1)
            val rid = bag.iterator().next()
            Assert.assertEquals(rid.clusterId, 57)
            Assert.assertEquals(rid.clusterPosition, 0)
        }
    }

    @Test
    fun testNestedLinkCreation() {
        val jaimeRid = db.computeInTx {
            val jaimeEntity = db.newEntity("NestedLinkCreation")
            jaimeEntity.setProperty("name", "jaime")
            jaimeEntity.identity
        }

        val cerseiRid = db.computeInTx {
            val jaimeEntity = db.loadEntity(jaimeRid)
            val cerseiEntity = db.newEntity("NestedLinkCreation")

            cerseiEntity.updateFromJSON(
                "{\"name\":\"cersei\",\"valonqar\":" + jaimeEntity.toJSON() + "}"
            )
            cerseiEntity.identity
        }


        val tyrionRid = db.computeInTx {
            // The link between jamie and tyrion is not saved properly
            val jaimeEntity = db.loadEntity(jaimeRid)
            val tyrionEntity = db.newEntity("NestedLinkCreation")

            tyrionEntity.updateFromJSON(
                ("{\"name\":\"tyrion\",\"emergency_contact\":{\"@type\":\"d\","
                        + " \"relationship\":\"brother\",\"contact\":"
                        + jaimeEntity.toJSON()
                        + "}}")
            )

            tyrionEntity.identity
        }


        checkJsonSerialization(jaimeRid)
        checkJsonSerialization(cerseiRid)
        checkJsonSerialization(tyrionRid)

        val jaimeMap = mapOf(
            "name" to "jaime",
            "@rid" to jaimeRid,
            "@class" to "NestedLinkCreation"
        )
        checkJsonSerialization(jaimeRid, jaimeMap)

        val cerseiMap = mapOf(
            "name" to "cersei",
            "valonqar" to jaimeRid,
            "@rid" to cerseiRid,
            "@class" to "NestedLinkCreation"
        )
        checkJsonSerialization(cerseiRid, cerseiMap)

        val tyrionMap = mapOf(
            "name" to "tyrion",
            "emergency_contact" to mapOf(
                "relationship" to "brother",
                "contact" to jaimeRid,
                "@embedded" to true
            ),
            "@rid" to tyrionRid,
            "@class" to "NestedLinkCreation"
        )
        checkJsonSerialization(tyrionRid, tyrionMap)
    }

    @Test
    fun testNestedLinkCreationFieldTypes() {
        val jaimeRid = db.computeInTx {
            val jaimeDoc = (db.newEntity("NestedLinkCreationFieldTypes") as EntityImpl)
            jaimeDoc.field("name", "jaime")
            jaimeDoc.identity
        }

        val cerseiRid = db.computeInTx {
            // The link between jaime and cersei is saved properly - the #2263 test case
            val cerseiDoc = db.newEntity("NestedLinkCreationFieldTypes")
            cerseiDoc.updateFromJSON(
                ("{\"@type\":\"d\"," +
                        "\"@fieldTypes\":{\"valonqar\" : \"x\"},\"name\":\"cersei\",\"valonqar\":\""
                        + jaimeRid
                        + "\"}")
            )
            cerseiDoc.identity
        }

        val tyrionRid = db.computeInTx {
            // The link between jamie and tyrion is not saved properly
            val tyrionDoc = (db.newEntity("NestedLinkCreationFieldTypes") as EntityImpl)
            tyrionDoc.updateFromJSON(
                "{\"@type\":\"d\",\"name\":\"tyrion\",\"emergency_contact\":{\"@type\":\"d\","
                        + " \"@fieldTypes\":{\"contact\":\"x\"},\"relationship\":\"brother\",\"contact\":\""
                        + jaimeRid + "\"}}"
            )
            tyrionDoc.identity
        }


        checkJsonSerialization(jaimeRid)
        checkJsonSerialization(cerseiRid)
        checkJsonSerialization(tyrionRid)

        val jaimeMap = mapOf(
            "name" to "jaime",
            "@rid" to jaimeRid,
            "@class" to "NestedLinkCreationFieldTypes"
        )
        checkJsonSerialization(jaimeRid, jaimeMap)

        val cerseiMap = mapOf(
            "name" to "cersei",
            "valonqar" to jaimeRid,
            "@rid" to cerseiRid,
            "@class" to "NestedLinkCreationFieldTypes"
        )
        checkJsonSerialization(cerseiRid, cerseiMap)

        val tyrionMap = mapOf(
            "name" to "tyrion",
            "emergency_contact" to mapOf(
                "relationship" to "brother",
                "contact" to jaimeRid,
                "@embedded" to true
            ),
            "@rid" to tyrionRid,
            "@class" to "NestedLinkCreationFieldTypes"
        )
        checkJsonSerialization(tyrionRid, tyrionMap)
    }

    @Test
    fun testInnerDocCreation() {
        val adamRid = db.computeInTx {
            val adamDoc = (db.newEntity("InnerDocCreation") as EntityImpl)
            adamDoc.updateFromJSON("{\"name\":\"adam\"}")

            adamDoc.identity
        }

        val eveRid = db.computeInTx {
            val adamDoc = db.loadEntity(adamRid)
            val eveDoc = (db.newEntity("InnerDocCreation") as EntityImpl)
            eveDoc.updateFromJSON(
                "{\"@type\":\"d\",\"name\":\"eve\",\"friends\":[" + adamDoc.toJSON() + "]}"
            )
            eveDoc.identity
        }

        checkJsonSerialization(adamRid)
        checkJsonSerialization(eveRid)

        val adamMap = mapOf(
            "name" to "adam",
            "@rid" to adamRid,
            "@class" to "InnerDocCreation"
        )
        checkJsonSerialization(adamRid, adamMap)

        val eveMap = mapOf(
            "name" to "eve",
            "friends" to listOf(adamRid),
            "@rid" to eveRid,
            "@class" to "InnerDocCreation"
        )
        checkJsonSerialization(eveRid, eveMap)
    }

    @Test
    fun testInnerDocCreationFieldTypes() {
        val (adamRid, eveRid) = db.computeInTx {
            val adamDoc = (db.newEntity("InnerDocCreationFieldTypes") as EntityImpl)
            adamDoc.updateFromJSON("{\"name\":\"adam\"}")

            val eveDoc = (db.newEntity("InnerDocCreationFieldTypes") as EntityImpl)
            eveDoc.updateFromJSON(
                ("{\"@type\":\"d\", \"@fieldTypes\" : { \"friends\":\"z\"}, \"name\":\"eve\",\"friends\":[\""
                        + adamDoc.identity
                        + "\"]}")
            )

            Pair(adamDoc.identity, eveDoc.identity)
        }

        checkJsonSerialization(adamRid)
        checkJsonSerialization(eveRid)

        val expectedAdamMap = mapOf(
            "name" to "adam",
            "@rid" to adamRid,
            "@class" to "InnerDocCreationFieldTypes"
        )
        checkJsonSerialization(adamRid, expectedAdamMap)

        val expectedEveMap = mapOf(
            "name" to "eve",
            "friends" to listOf(adamRid),
            "@rid" to eveRid,
            "@class" to "InnerDocCreationFieldTypes"
        )
        checkJsonSerialization(eveRid, expectedEveMap)
    }


    @Test
    fun testInvalidLink() {
        try {
            db.executeInTx {
                val nullRefDoc = (db.newEntity() as EntityImpl)
                nullRefDoc.updateFromJSON("{\"name\":\"Luca\", \"ref\":\"#-1:-1\"}")
            }
            Assert.fail()
        } catch (e: CommitSerializationException) {
            // expected
        }
    }


    @Test
    fun testOtherJson() {
        val rid = db.computeInTx {
            val entity = db.newEntity()
            entity.updateFromJSON(
                ("{\"Salary\":1500.0,\"Type\":\"Person\",\"Address\":[{\"Zip\":\"JX2"
                        + " MSX\",\"Type\":\"Home\",\"Street1\":\"13 Marge"
                        + " Street\",\"Country\":\"Holland\",\"Id\":\"Address-28813211\",\"City\":\"Amsterdam\",\"From\":\"1996-02-01\",\"To\":\"1998-01-01\"},{\"Zip\":\"90210\",\"Type\":\"Work\",\"Street1\":\"100"
                        + " Hollywood"
                        + " Drive\",\"Country\":\"USA\",\"Id\":\"Address-11595040\",\"City\":\"Los"
                        + " Angeles\",\"From\":\"2009-09-01\"}],\"Id\":\"Person-7464251\",\"Name\":\"Stan\"}")
            )
            entity.identity
        }
        checkJsonSerialization(rid)
        val map = mapOf(
            "Salary" to 1500.0,
            "Type" to "Person",
            "Address" to listOf(
                mapOf(
                    "Zip" to "JX2 MSX",
                    "Type" to "Home",
                    "Street1" to "13 Marge Street",
                    "Country" to "Holland",
                    "Id" to "Address-28813211",
                    "City" to "Amsterdam",
                    "From" to "1996-02-01",
                    "To" to "1998-01-01"
                ),
                mapOf(
                    "Zip" to "90210",
                    "Type" to "Work",
                    "Street1" to "100 Hollywood Drive",
                    "Country" to "USA",
                    "Id" to "Address-11595040",
                    "City" to "Los Angeles",
                    "From" to "2009-09-01"
                )
            ),
            "Id" to "Person-7464251",
            "Name" to "Stan",
            "@rid" to rid,
            "@class" to "O"
        )
        checkJsonSerialization(rid, map)
    }

    @Test
    fun testScientificNotation() {
        val rid = db.computeInTx {
            val doc = (db.newEntity() as EntityImpl)
            doc.updateFromJSON("{\"number1\": -9.2741500e-31, \"number2\": 741800E+290}")
            doc.identity
        }

        checkJsonSerialization(rid)
        val expectedMap = mapOf(
            "number1" to -9.27415E-31,
            "number2" to 741800E+290,
            "@rid" to rid,
            "@class" to "O"
        )
        checkJsonSerialization(rid, expectedMap)
    }

    private fun checkJsonSerialization(rid: RID) {
        db.begin()
        try {
            val original = db.loadEntity(rid)
            val json = original.toJSON(FORMAT_WITHOUT_RID)

            val newEntity = db.entityFromJson(json)

            val originalMap = original.toMap()
            val loadedMap = newEntity.toMap()

            originalMap.remove(EntityHelper.ATTRIBUTE_RID)
            loadedMap.remove(EntityHelper.ATTRIBUTE_RID)

            Assert.assertEquals(originalMap, loadedMap)
        } finally {
            db.rollback()
        }
    }

    private fun checkJsonSerialization(rid: RID, expectedMap: Map<String, Any>) {
        db.executeInTx {
            val original = db.loadEntity(rid)
            val originalMap = original.toMap()
            Assert.assertEquals(originalMap, expectedMap)
        }
    }

    companion object {
        const val FORMAT_WITHOUT_RID: String = "version,class,type,keepTypes"
    }
}
