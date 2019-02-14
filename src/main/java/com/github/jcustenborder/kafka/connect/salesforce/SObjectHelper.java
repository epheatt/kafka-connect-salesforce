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
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectDescriptor;
import com.github.jcustenborder.kafka.connect.utils.data.Parser;
import com.github.jcustenborder.kafka.connect.utils.data.type.TimestampTypeParser;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.utils.SystemTime;
import org.apache.kafka.common.utils.Time;
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

	final SalesforceSourceConnectorConfig config;
	final Schema keySchema;
	final Schema valueSchema;
	final Map<String, Object> sourcePartition;
	final static Map<String,String> binaryCharMap = new HashMap<>();
	Time time = new SystemTime();

	static final String ADDRESS_SCHEMA_NAME = String.format("%s.%s", SObjectHelper.class.getPackage().getName(),
			"Address");

	static {
		Parser p = new Parser();
		p.registerTypeParser(Timestamp.SCHEMA,
				new TimestampTypeParser(TimeZone.getTimeZone("UTC"), new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")));
		PARSER = p;
		binaryCharMap.put("00000","A");
		binaryCharMap.put("00001","B");
		binaryCharMap.put("00010","C");
		binaryCharMap.put("00011","D");
		binaryCharMap.put("00100","E");
		binaryCharMap.put("00101","F");
		binaryCharMap.put("00110","G");
		binaryCharMap.put("00111","H");
		binaryCharMap.put("01000","I");
		binaryCharMap.put("01001","J");
		binaryCharMap.put("01010","K");
		binaryCharMap.put("01011","L");
		binaryCharMap.put("01100","M");
		binaryCharMap.put("01101","N");
		binaryCharMap.put("01110","O");
		binaryCharMap.put("01111","P");
		binaryCharMap.put("10000","Q");
		binaryCharMap.put("10001","R");
		binaryCharMap.put("10010","S");
		binaryCharMap.put("10011","T");
		binaryCharMap.put("10100","U");
		binaryCharMap.put("10101","V");
		binaryCharMap.put("10110","W");
		binaryCharMap.put("10111","X");
		binaryCharMap.put("11000","Y");
		binaryCharMap.put("11001","Z");
		binaryCharMap.put("11010","0");
		binaryCharMap.put("11011","1");
		binaryCharMap.put("11100","2");
		binaryCharMap.put("11101","3");
		binaryCharMap.put("11110","4");
		binaryCharMap.put("11111","5");
	}

	public SObjectHelper(SalesforceSourceConnectorConfig config, Schema keySchema, Schema valueSchema) {
		this.config = config;
		this.keySchema = keySchema;
		this.valueSchema = valueSchema;
		this.sourcePartition = ImmutableMap.of("pushTopic", this.config.salesForcePushTopicName);
	}

	public static boolean isTextArea(SObjectDescriptor.Field field) {
		return "textarea".equalsIgnoreCase(field.type());
	}

	public static Schema schema(SObjectDescriptor.Field field) {
		SchemaBuilder builder = null;

		boolean optional = true;
		boolean referenceType = false;

		switch (field.type()) {
		case "id":
			optional = false;
			builder = SchemaBuilder.string().doc("Unique identifier for the object.");
			break;
		case "boolean":
			builder = SchemaBuilder.bool();
			break;
		case "date":
			builder = SchemaBuilder.string().doc("Value will be in Date string format.");
			break;
		case "address":
			builder = SchemaBuilder.struct().name(ADDRESS_SCHEMA_NAME)
					.field("GeocodeAccuracy", SchemaBuilder.string().optional().build())
					.field("State", SchemaBuilder.string().optional().doc("").build())
					.field("Street", SchemaBuilder.string().optional().build())
					.field("PostalCode", SchemaBuilder.string().optional().build())
					.field("Country", SchemaBuilder.string().optional().build())
					.field("Latitude", SchemaBuilder.float64().optional().build())
					.field("City", SchemaBuilder.string().optional().build())
					.field("Longitude", SchemaBuilder.float64().optional().build());
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
			referenceType = true;
			break;
		case "datetime":
			builder = SchemaBuilder.string().doc("Value will be in DateTime string format.");
			break;
		case "phone":
			builder = SchemaBuilder.string();
			break;
		case "currency":
			builder = SchemaBuilder.float64();
			break;
		case "email":
			builder = SchemaBuilder.string();
			break;
		case "decimal":
			builder = Decimal.builder(field.scale());
			break;
		case "percent":
                        builder = SchemaBuilder.float64();
                        break;
		case "multipicklist":
                        builder = SchemaBuilder.string();
                        break;
		default:
			throw new UnsupportedOperationException(
					String.format("Field type '%s' for field '%s' is not supported", field.type(), field.name()));
		}

		if (optional) {
			builder = builder.optional();
		}
                if (referenceType) {
                        builder = builder.parameter("referenceType", "true");
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

		builder.field(FIELD_OBJECT_TYPE, Schema.OPTIONAL_STRING_SCHEMA);
		builder.field(FIELD_EVENT_TYPE, Schema.OPTIONAL_STRING_SCHEMA);

		return builder.build();
	}

	public static final String FIELD_OBJECT_TYPE = "_ObjectType";
	public static final String FIELD_EVENT_TYPE = "_EventType";

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

	public void convertStruct(JsonNode sObjectNode, Schema schema, Struct struct) {
		for (Field field : schema.fields()) {
			String fieldName = field.name();
			JsonNode valueNode = sObjectNode.findValue(fieldName);

			Object value;
			if (ADDRESS_SCHEMA_NAME.equals(field.schema().name()) && valueNode != null) {
				Struct address = new Struct(field.schema());
				for (Field addressField : field.schema().fields()) {
					JsonNode fieldValueNode = valueNode.findValue(addressField.name());
					Object fieldValue = PARSER.parseJsonNode(addressField.schema(), fieldValueNode);
					address.put(addressField, fieldValue);
				}
				value = address;
			} else {
				value = PARSER.parseJsonNode(field.schema(), valueNode);
			}

                        if (field.schema().parameters() != null && field.schema().parameters().containsKey("referenceType")) {
                                 value = convertReferenceFieldValue(value);
                        }
			struct.put(field, value);
		}
	}

    private Object convertReferenceFieldValue(Object value) {
        if (value != null && value instanceof String && value.toString().length() == 15) {
            String id = value.toString();
            StringBuilder part1 = new StringBuilder(id.substring(0, 5));
            StringBuilder part2 = new StringBuilder(id.substring(5, 10));
            StringBuilder part3 = new StringBuilder(id.substring(10, 15));
            String rPart1 = part1.reverse().toString();
            String rPart2 = part2.reverse().toString();
            String rPart3 = part3.reverse().toString();
            StringBuilder convertedVB = new StringBuilder(id);
            convertedVB.append(getAlphabetForString(rPart1));
            convertedVB.append(getAlphabetForString(rPart2));
            convertedVB.append(getAlphabetForString(rPart3));
            return convertedVB.toString();
        } else {
            return value;
        }
    }

    private String getAlphabetForString(String rPart) {
        StringBuilder builder = new StringBuilder();
        for (Character c : rPart.toCharArray()) {
            if (Character.isUpperCase(c)) {
                builder.append("1");
            } else {
                builder.append("0");
            }
        }
        String ch = binaryCharMap.get(builder.toString());
        if (ch != null) {
            return ch;
        }
        return "";
    }



	public SourceRecord convert(JsonNode jsonNode) {
		Preconditions.checkNotNull(jsonNode);
		Preconditions.checkState(jsonNode.isObject());
		JsonNode dataNode = jsonNode.get("data");
		JsonNode eventNode = dataNode.get("event");
		JsonNode sobjectNode = dataNode.get("sobject");
		final long replayId = eventNode.get("replayId").asLong();
		final String eventType = eventNode.get("type").asText();
		Struct keyStruct = new Struct(keySchema);
		Struct valueStruct = new Struct(valueSchema);
		convertStruct(sobjectNode, keySchema, keyStruct);
		convertStruct(sobjectNode, valueSchema, valueStruct);
		valueStruct.put(FIELD_OBJECT_TYPE, this.config.salesForceObject);
		valueStruct.put(FIELD_EVENT_TYPE, eventType);

		String topic = this.config.kafkaTopicTemplate.execute(SalesforceSourceConnectorConfig.TEMPLATE_NAME,
				valueStruct);
		if (this.config.kafkaTopicLowerCase) {
			topic = topic.toLowerCase();
		}
		Map<String, Long> sourceOffset = ImmutableMap.of("replayId", replayId);
		return new SourceRecord(SOURCE_PARTITIONS, sourceOffset, topic, null, this.keySchema, keyStruct,
				this.valueSchema, valueStruct, this.time.milliseconds());
	}

}
