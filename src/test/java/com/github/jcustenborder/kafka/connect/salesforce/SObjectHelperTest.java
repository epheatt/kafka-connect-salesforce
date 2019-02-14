/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jcustenborder.kafka.connect.salesforce;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectDescriptor;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import org.apache.kafka.connect.data.*;
import org.apache.kafka.connect.json.JsonSchema;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;


public class SObjectHelperTest {
  private static final Logger log = LoggerFactory.getLogger(SObjectHelperTest.class);
  private static final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void valueSchema() {
    final SObjectDescriptor descriptor = TestData.sObjectDescriptor();
    final Schema schema = SObjectHelper.valueSchema(descriptor);
    assertNotNull(schema);
    assertEquals("com.github.jcustenborder.kafka.connect.salesforce.Lead", schema.name());
  }

  @Test
  public void testConvertSchemaless() {
    Map<String, String> settings = TestData.settings();
    settings.put(SalesforceSourceConnectorConfig.SCHEMAS_ENABLE_CONFIG,"false");
    final SalesforceSourceConnectorConfig config = new SalesforceSourceConnectorConfig(settings);
    final SObjectDescriptor descriptor = TestData.sObjectDescriptor();
    final Schema keySchema = SObjectHelper.keySchema(descriptor);
    final Schema valueSchema = SObjectHelper.valueSchema(descriptor);
    SObjectHelper helper = new SObjectHelper(config, keySchema, valueSchema);
    JsonNode jsonNode = TestData.salesForcePushTopic();
    SourceRecord sr = helper.convert(jsonNode);
    assertNotNull(sr.value());
  }

  @Test
  @Deprecated
  public void testChangeEvents() throws com.fasterxml.jackson.core.JsonProcessingException {
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(SalesforceSourceConnectorConfig.CONSUMER_KEY_CONF, "sdfasdfasd");
    settings.put(SalesforceSourceConnectorConfig.CONSUMER_SECRET_CONF, "sdfasdfasd");
    settings.put(SalesforceSourceConnectorConfig.PASSWORD_CONF, "sdfasdfasd");
    settings.put(SalesforceSourceConnectorConfig.PASSWORD_TOKEN_CONF, "sdfasdfasd");
    settings.put(SalesforceSourceConnectorConfig.USERNAME_CONF, "Account");
    settings.put(SalesforceSourceConnectorConfig.SCHEMAS_ENABLE_CONFIG,"false");
    settings.put(SalesforceSourceConnectorConfig.SALESFORCE_CHANGE_EVENT_ENABLE_CONFIG, "true");
    settings.put(SalesforceSourceConnectorConfig.SALESFORCE_CHANNEL_CONF, "/data/ChangeEvents");
    settings.put(SalesforceSourceConnectorConfig.KAFKA_TOPIC_CONF, "salesforce.Account");
    JsonNode changeEvent = TestData.salesForceChangeEvent();
    JsonNode eventSchema = TestData.salesForceEventSchema();
    /*
    Map<String, Object> conf = new HashMap<>();
    conf.put("enhanced.avro.schema.support","false");
    JsonConverter jsonConverter = new JsonConverter();
    AvroData avroData = new AvroData(new AvroDataConfig(conf));
    jsonConverter.configure(conf,false);
    ObjectMapper mapper = new ObjectMapper();
    org.apache.avro.Schema avroSchema = org.apache.avro.Schema.parse(mapper.writeValueAsString(eventSchema));
    Schema connectSchema = avroData.toConnectSchema(avroSchema);
    ObjectNode jsonSchema = jsonConverter.asJsonSchema(connectSchema);
    ObjectNode source = mapper.createObjectNode();
    source.put("schema",jsonSchema);
    source.put("payload",changeEvent.get("data").get("payload"));
    JsonSerializer serializer = new JsonSerializer();
    SchemaAndValue connectData = jsonConverter.toConnectData("test",serializer.serialize("test",source));
    */
    SalesforceSourceConnectorConfig config = new SalesforceSourceConnectorConfig(settings);
    SObjectHelper helper = new SObjectHelper(config);
    SourceRecord sr = helper.convert(changeEvent);
    assertNotNull(sr.value());
  }

  @Test
  public void convertJson() {
    final SObjectDescriptor descriptor = TestData.sObjectDescriptor();
    JsonNode jsonNode = TestData.salesForcePushTopic();
    JsonNode dataNode = jsonNode.get("data");
    JsonNode eventNode = dataNode.get("event");
    JsonNode sobjectNode = dataNode.get("sobject");
    final Schema valueSchema = SObjectHelper.valueSchema(descriptor);
    final Schema keySchema = SObjectHelper.keySchema(descriptor);
    JsonConverter jsonConverter = new JsonConverter();
    JsonSerializer serializer = new JsonSerializer();
    Map<String, Object> conf = new HashMap<>();
    jsonConverter.configure(conf,false);
    serializer.configure(conf, false);
    JsonNode jsonSchema = jsonConverter.asJsonSchema(valueSchema);
    ObjectNode record = mapper.createObjectNode();
    //Object projected = SchemaProjector.project(SchemaBuilder.struct().name("com.github.jcustenborder.kafka.connect.salesforce.Lead").build(),sobjectNode,valueSchema);
    Struct struct = new Struct(valueSchema);
    SalesforceSourceConnectorConfig config = new SalesforceSourceConnectorConfig(TestData.settings());
    SObjectHelper helper = new SObjectHelper(config, keySchema, valueSchema);
    record.set("schema",jsonSchema);
    ObjectNode payload = (ObjectNode) helper.defaultMissingOptional(valueSchema, dataNode.get("sobject"));
    payload.put("_ObjectType","Lead");
    payload.set("_EventType",eventNode.get("type"));
    record.set("payload",payload);
    SchemaAndValue connectData = jsonConverter.toConnectData("topic",serializer.serialize("topic",record));
    //byte[] data = jsonConverter.fromConnectData("topic",connectData.schema(),connectData.value());
    assertNotNull(connectData.value());
  }

  @Test
  public void convertStruct() {
    final SObjectDescriptor descriptor = TestData.sObjectDescriptor();
    JsonNode jsonNode = TestData.salesForcePushTopic();
    JsonNode dataNode = jsonNode.get("data");
    JsonNode eventNode = dataNode.get("event");
    JsonNode sobjectNode = dataNode.get("sobject");
    final Schema valueSchema = SObjectHelper.valueSchema(descriptor);
    final Schema keySchema = SObjectHelper.keySchema(descriptor);
    Struct struct = new Struct(valueSchema);
    SalesforceSourceConnectorConfig config = new SalesforceSourceConnectorConfig(TestData.settings());
    SObjectHelper helper = new SObjectHelper(config, keySchema, valueSchema);
    helper.convertStruct(sobjectNode, valueSchema, struct);
    assertNotNull(struct);
  }

  @BeforeAll
  public static void objectMapperConfig() {
    ObjectMapperFactory.INSTANCE.configure(SerializationFeature.INDENT_OUTPUT, true);
  }
}
