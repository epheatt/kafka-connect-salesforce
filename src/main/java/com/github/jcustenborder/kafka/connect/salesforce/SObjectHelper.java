/**
 * Copyright © 2016 Jeremy Custenborder (jcustenborder@gmail.com)
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
package com.github.jcustenborder.kafka.connect.salesforce;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.jcustenborder.kafka.connect.utils.data.Parser;
import com.github.jcustenborder.kafka.connect.utils.data.type.DateTypeParser;
import com.google.api.client.util.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectDescriptor;
import org.apache.kafka.connect.data.Date;
import org.apache.kafka.connect.data.Decimal;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

class SObjectHelper {
  static final Parser PARSER;
  static final Map<String, ?> SOURCE_PARTITIONS = new HashMap<>();
  private static final Logger log = LoggerFactory.getLogger(SObjectHelper.class);

  static {
    Parser p = new Parser();
//    "2016-08-15T22:07:59.000Z"
    p.registerTypeParser(Timestamp.SCHEMA, new DateTypeParser(TimeZone.getTimeZone("UTC"), new SimpleDateFormat("YYYY-MM-dd'T'HH:mm:ss.SSS'Z'")));
    PARSER = p;
  }

  public static boolean isTextArea(SObjectDescriptor.Field field) {
    return "textarea".equalsIgnoreCase(field.type());
  }

  public static Schema schema(SObjectDescriptor.Field field) {
    SchemaBuilder builder = null;

    boolean optional = true;

    switch (field.type()) {
      case "id":
        optional = false;
        builder = SchemaBuilder.string().doc("Unique identifier for the object.");
        break;
      case "boolean":
        builder = SchemaBuilder.bool();
        break;
      case "date":
        builder = Date.builder();
        break;
      case "address":
        builder = SchemaBuilder.string();
        break;
      case "string":
        builder = SchemaBuilder.string();
        break;
      case "double":
        builder = SchemaBuilder.float64();
        break;
      case "picklist":
        builder = SchemaBuilder.string();
        break;
      case "textarea":
        builder = SchemaBuilder.string();
        break;
      case "url":
        builder = SchemaBuilder.string();
        break;
      case "int":
        builder = SchemaBuilder.int32();
        break;
      case "reference":
        builder = SchemaBuilder.string();
        break;
      case "datetime":
        builder = Timestamp.builder();
        break;
      case "phone":
        builder = SchemaBuilder.string();
        break;
      case "currency":
        builder = SchemaBuilder.string();
        break;
      case "email":
        builder = SchemaBuilder.string();
        break;
      case "decimal":
        builder = Decimal.builder(field.scale());
        break;
      default:
        throw new UnsupportedOperationException(
            String.format("Field type '%s' for field '%s' is not supported", field.type(), field.name())
        );
    }

    if (optional) {
      builder = builder.optional();
    }

    return builder.build();
  }

  public static Schema valueSchema(SObjectDescriptor descriptor) {
    String name = String.format("%s.%s", SObjectHelper.class.getPackage().getName(), descriptor.name());
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(name);

    for (SObjectDescriptor.Field field : descriptor.fields()) {
      if (isTextArea(field)) {
        continue;
      }
      Schema schema = schema(field);
      builder.field(field.name(), schema);
    }

    return builder.build();
  }

  public static Schema keySchema(SObjectDescriptor descriptor) {
    String name = String.format("%s.%sKey", SObjectHelper.class.getPackage().getName(), descriptor.name());
    SchemaBuilder builder = SchemaBuilder.struct();
    builder.name(name);

    SObjectDescriptor.Field keyField = null;

    for (SObjectDescriptor.Field field : descriptor.fields()) {
      if ("id".equalsIgnoreCase(field.type())) {
        keyField = field;
        break;
      }
    }

    if (null == keyField) {
      throw new IllegalStateException("Could not find an id field for " + descriptor.name());
    }

    Schema keySchema = schema(keyField);
    builder.field(keyField.name(), keySchema);
    return builder.build();
  }

  public static void convertStruct(JsonNode data, Schema schema, Struct struct) {
    for (Field field : schema.fields()) {
      String fieldName = field.name();
      JsonNode valueNode = data.findValue(fieldName);
      Object value = PARSER.parseJsonNode(field.schema(), valueNode);
      struct.put(field, value);
    }
  }

  public static SourceRecord convert(JsonNode jsonNode, String pushTopicName, String topic, Schema keySchema, Schema valueSchema) {
    Preconditions.checkNotNull(jsonNode);
    Preconditions.checkState(jsonNode.isObject());
    JsonNode dataNode = jsonNode.get("data");
    JsonNode eventNode = dataNode.get("event");
    JsonNode sobjectNode = dataNode.get("sobject");
    long replayId = eventNode.get("replayId").asLong();
    Struct keyStruct = new Struct(keySchema);
    Struct valueStruct = new Struct(valueSchema);
    convertStruct(sobjectNode, keySchema, keyStruct);
    convertStruct(sobjectNode, valueSchema, valueStruct);
    Map<String, Long> sourceOffset = ImmutableMap.of(pushTopicName, replayId);
    return new SourceRecord(SOURCE_PARTITIONS, sourceOffset, topic, keySchema, keyStruct, valueSchema, valueStruct);
  }

}