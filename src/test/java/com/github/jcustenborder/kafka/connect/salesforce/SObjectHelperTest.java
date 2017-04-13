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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectDescriptor;
import com.github.jcustenborder.kafka.connect.utils.jackson.ObjectMapperFactory;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;


public class SObjectHelperTest {
  private static final Logger log = LoggerFactory.getLogger(SObjectHelperTest.class);

  @Test
  public void valueSchema() {
    final SObjectDescriptor descriptor = TestData.sObjectDescriptor();
    final Schema schema = SObjectHelper.valueSchema(descriptor);
    assertNotNull(schema);
    assertEquals("com.github.jcustenborder.kafka.connect.salesforce.Lead", schema.name());
  }

  @Test
  public void convertStruct() {
    final SObjectDescriptor descriptor = TestData.sObjectDescriptor();
    JsonNode jsonNode = TestData.salesForceEvent();
    JsonNode dataNode = jsonNode.get("data");
    JsonNode eventNode = jsonNode.get("event");
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
