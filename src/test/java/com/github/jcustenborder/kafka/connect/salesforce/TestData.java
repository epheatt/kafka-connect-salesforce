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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectDescriptor;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.Map;

public class TestData {
  private static final JsonFactory jsonFactory = new JacksonFactory();
  private static final ObjectMapper objectMapper = new ObjectMapper();

  public static SObjectDescriptor sObjectDescriptor() {
    try (InputStream inputStream = TestData.class.getResourceAsStream("sobjectdescriptor.json")) {
      return jsonFactory.fromInputStream(inputStream, SObjectDescriptor.class);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not read", ex);
    }
  }

  public static JsonNode salesForceEvent() {
    try (InputStream inputStream = TestData.class.getResourceAsStream("salesforceevent.json")) {
      return objectMapper.readTree(inputStream);
    } catch (IOException ex) {
      throw new IllegalStateException("Could not read", ex);
    }
  }

  public static Map<String, String> settings() {
    Map<String, String> settings = new LinkedHashMap<>();
    settings.put(SalesforceSourceConnectorConfig.CONSUMER_KEY_CONF, "sdfasdfasd");
    settings.put(SalesforceSourceConnectorConfig.CONSUMER_SECRET_CONF, "sdfasdfasd");
    settings.put(SalesforceSourceConnectorConfig.PASSWORD_CONF, "sdfasdfasd");
    settings.put(SalesforceSourceConnectorConfig.PASSWORD_TOKEN_CONF, "sdfasdfasd");
    settings.put(SalesforceSourceConnectorConfig.SALESFORCE_OBJECT_CONF, "Lead");
    settings.put(SalesforceSourceConnectorConfig.USERNAME_CONF, "Lead");
    settings.put(SalesforceSourceConnectorConfig.KAFKA_TOPIC_CONF, "salesforce.${__ObjectType}");
    settings.put(SalesforceSourceConnectorConfig.SALESFORCE_PUSH_TOPIC_NAME_CONF, "Testing");
    return settings;
  }

}
