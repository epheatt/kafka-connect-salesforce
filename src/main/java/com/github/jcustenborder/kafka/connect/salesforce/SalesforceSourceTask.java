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

import com.github.jcustenborder.kafka.connect.salesforce.rest.SalesforceRestClient;
import com.github.jcustenborder.kafka.connect.salesforce.rest.SalesforceRestClientFactory;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.ApiVersion;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.AuthenticationResponse;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectDescriptor;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectMetadata;
import com.github.jcustenborder.kafka.connect.salesforce.rest.model.SObjectsResponse;
import com.google.api.client.http.GenericUrl;
import com.google.common.base.Preconditions;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.cometd.bayeux.Channel;
import org.cometd.bayeux.Message;
import org.cometd.bayeux.client.ClientSessionChannel;
import org.cometd.client.BayeuxClient;
import org.cometd.client.transport.LongPollingTransport;
import org.eclipse.jetty.client.HttpClient;
import org.eclipse.jetty.client.api.Request;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import org.cometd.bayeux.client.ClientSessionChannel.ClientSessionChannelListener;

public class SalesforceSourceTask extends SourceTask {
    static final Logger log = LoggerFactory.getLogger(SalesforceSourceTask.class);
    final BlockingQueue<SourceRecord> messageQueue = new LinkedBlockingQueue<>();
    SalesforceSourceConnectorConfig config;
    SalesforceRestClient salesforceRestClient;
    AuthenticationResponse authenticationResponse;
    SObjectDescriptor descriptor;
    SObjectMetadata metadata;
    ApiVersion apiVersion;
    GenericUrl streamingUrl;
    BayeuxClient streamingClient;
    HttpClient httpClient;
    Schema keySchema;
    Schema valueSchema;
    private final ConcurrentMap<String, Long> replay = new ConcurrentHashMap<>();
    ClientSessionChannelListener handshakeListener;
    ClientSessionChannelListener listener;
    private volatile boolean isConnectorTaskRunning;
    private volatile AtomicInteger authTaskCounter = new AtomicInteger(0);

    @Override
    public String version() {
        return VersionUtil.getVersion();
    }

    BayeuxClient createClient() {
        SslContextFactory sslContextFactory = new SslContextFactory();
        httpClient = new HttpClient(sslContextFactory);
        httpClient.setConnectTimeout(this.config.connectTimeout);
        try {
            httpClient.start();
        } catch (Exception e) {
            throw new ConnectException("Exception thrown while starting httpClient.", e);
        }

        Map<String, Object> options = new HashMap<>();

        LongPollingTransport transport = new LongPollingTransport(options, httpClient) {

            @Override
            protected void customize(Request request) {
                super.customize(request);
                String headerValue = String.format("Authorization: %s %s", authenticationResponse.tokenType(),
                        authenticationResponse.accessToken());
                request.header("Authorization", headerValue);
            }
        };

        return new BayeuxClient(this.streamingUrl.toString(), transport);
    }

    ClientSessionChannel topicChannel;
    TopicChannelMessageListener topicChannelListener;

    @Override
    public void start(Map<String, String> map) {
        this.isConnectorTaskRunning = true;
        this.config = new SalesforceSourceConnectorConfig(map);
        this.salesforceRestClient = SalesforceRestClientFactory.create(this.config);
        this.authenticationResponse = this.salesforceRestClient.authenticate();

        List<ApiVersion> apiVersions = salesforceRestClient.apiVersions();

        for (ApiVersion v : apiVersions) {
            if (this.config.version.equals(v.version())) {
                apiVersion = v;
                break;
            }
        }

        Preconditions.checkNotNull(apiVersion, "Could not find ApiVersion '%s'", this.config.version);
        salesforceRestClient.apiVersion(apiVersion);

        if (!this.config.salesForceChangeEventEnable) {
        SObjectsResponse sObjectsResponse = salesforceRestClient.objects();

        if (log.isInfoEnabled()) {
            log.info("Looking for metadata for {}", this.config.salesForceObject);
        }

        for (SObjectMetadata metadata : sObjectsResponse.sobjects()) {
            if (this.config.salesForceObject.equals(metadata.name())) {
                this.descriptor = salesforceRestClient.describe(metadata);
                this.metadata = metadata;
                break;
            }
        }

        // 2013-05-06T00:00:00+00:00
        Preconditions.checkNotNull(this.descriptor, "Could not find descriptor for '%s'", this.config.salesForceObject);

        this.keySchema = SObjectHelper.keySchema(this.descriptor);
        this.valueSchema = SObjectHelper.valueSchema(this.descriptor);
        } else {
            this.keySchema = Schema.STRING_SCHEMA;
            this.valueSchema = Schema.STRING_SCHEMA;
        }
        this.topicChannelListener = new TopicChannelMessageListener(this.messageQueue, this.config, this.keySchema,
                this.valueSchema);

        this.streamingUrl = new GenericUrl(this.authenticationResponse.instance_url());
        this.streamingUrl.setRawPath(String.format("/cometd/%s", this.apiVersion.version()));

        final String channel = this.config.salesForceChannel;//String.format("/topic/%s", this.config.salesForcePushTopicName);

        if (log.isInfoEnabled()) {
            log.info("Configuring streaming url to {}", this.streamingUrl);
        }

        this.listener = new ClientSessionChannel.MessageListener() {

            @Override
            public void onMessage(ClientSessionChannel ch, Message msg) {

                if (Channel.META_CONNECT.equals(ch.getId())) {
                    if (!msg.isSuccessful()) {
                        log.info("kafka topic {}'s connector, {} channel got message={}",config.kafkaTopic, ch.getId(), msg);
                        createNewClientOnReconnectNoneAdvice(ch, msg);
                    }
                } else if (Channel.META_SUBSCRIBE.equals(ch.getId())) {
                    if (!msg.isSuccessful() && msg.get("error").toString().contains("400::The replayId")) {
                        log.info("kafka topic {}'s connector Resetting replayId as -1 because error occurred. Msg={}",config.kafkaTopic,msg);
                        if (!topicChannel.getSubscribers().isEmpty()) {
                            topicChannel.unsubscribe(topicChannelListener);
                            log.info("Previous Message Listner unsubscribed from channel {}", channel);
                        }
                        replay.put(channel, -1L);
                        topicChannel.subscribe(topicChannelListener);
                        log.info("Message Listner again subscribed to channel {}", channel);

                    }
                } else {
                    log.debug("{} channel got message={}", ch.getId(), msg);
                }
            }
        };

        this.handshakeListener = new ClientSessionChannel.MessageListener() {

            @Override
            public void onMessage(ClientSessionChannel clientSessionChannel, Message message) {
                log.info("kafka topic {}'s connector onMessage(META_HANDSHAKE) - {}",config.kafkaTopic, message);

                if (message.isSuccessful()) {
                    if (null == topicChannel) {
                        log.trace("onMessage(META_HANDSHAKE) - This is the first call to the topic channel.");
                        topicChannel = streamingClient.getChannel(channel);
                    }

                    if (topicChannel.getSubscribers().isEmpty()) {
                        setReplayId(channel);
                        log.info("onMessage(META_HANDSHAKE) - Subscribing to {}", channel);
                        topicChannel.subscribe(topicChannelListener);
                    } else {
                        log.warn("onMessage(META_HANDSHAKE) - Already subscribed.");
                    }
                } else if (message.get("error") != null && "401::Authentication invalid".equalsIgnoreCase(message.get("error").toString())) {
                    log.error("Authentication Error - Initiating authentication again...");
                    authenticate();
                } else {
                    log.error("kafka topic {}'s connector Error during handshake: {} {}",config.kafkaTopic, message.get("error"), message.get("exception"));
                    createNewClientOnReconnectNoneAdvice(clientSessionChannel, message);
                }
            }

            private void setReplayId(final String channel) {
                replay.clear();
                OffsetStorageReader offsetReader = context.offsetStorageReader();
                Map<String, Object> partitionOffset = offsetReader.offset(new HashMap<>());
                if (partitionOffset != null && partitionOffset.get("replayId") instanceof Long) {
                    Long sourceOffset = (Long) partitionOffset.get("replayId");
                    log.info("kafka topic {}'s connector channel {} - found stored offset {}",config.kafkaTopic, channel, sourceOffset);
                    replay.put(channel, sourceOffset);
                }
            }
        };

        createClientAndSubscribeListener();
    }

    private void createNewClientOnReconnectNoneAdvice(ClientSessionChannel channel, Message msg) {
        if (isConnectorTaskRunning) {
            Map<String, Object> advices;
            advices = msg.getAdvice();
            if (advices != null && advices.containsKey(Message.RECONNECT_FIELD)) {
                String action = (String) advices.get(Message.RECONNECT_FIELD);
                if (Message.RECONNECT_NONE_VALUE.equals(action)) {
                    boolean isConnected = streamingClient.waitFor(2L, BayeuxClient.State.CONNECTED);
                    log.info("kafka topic {}'s connector's {} :Re-Creating of client as reconnect=none received. client state {}",config.kafkaTopic, channel.getId(), isConnected);
                    authenticate();
                }
            }
        } else {
            log.info("Connector task is not running so not checking reconnect none advice.");
        }
    }

    private void authenticate() {
        log.info("kafka topic {}'s connector's total authentication in queue {}.",config.kafkaTopic,authTaskCounter.incrementAndGet());
        try {
            //this sleep is added so that if previous authentication is going on then it should be complete.
            Thread.sleep(config.wait_in_ms);
        } catch (Exception e) {
            log.error("error while waiting before authentication.", e);
        }
        //multiple authentication should not happen.
        synchronized (this) {
            //only go for authentication if client is not connected.
            if(!streamingClient.waitFor(10L, BayeuxClient.State.CONNECTED)){
                authenticationResponse = salesforceRestClient.authenticate();
                streamingUrl = new GenericUrl(authenticationResponse.instance_url());
                streamingUrl.setRawPath(String.format("/cometd/%s", this.apiVersion.version()));
                createClientAndSubscribeListener();
            } else {
                authTaskCounter.decrementAndGet();
            }
        }
    }

    // This code will try for max configured retry attempts for reaching to connected status of BayeuxClient...
    private void createClientAndSubscribeListener() {
        topicChannel = null;
        if (streamingClient != null) {
            try {
                streamingClient.abort();
                log.info("Old client aborted.");
            } catch (Exception e) {
                log.info("Old client abort Error - {}", e.getMessage(),e);
            }
        }

        streamingClient = createClient();
        streamingClient.getChannel(Channel.META_CONNECT).addListener(listener);
        streamingClient.getChannel(Channel.META_DISCONNECT).addListener(listener);
        streamingClient.getChannel(Channel.META_SUBSCRIBE).addListener(listener);
        streamingClient.getChannel(Channel.META_HANDSHAKE).addListener(handshakeListener);
        replay.clear();
        streamingClient.addExtension(new ReplayExtension(replay));
        log.info("kafka topic {}'s connector starting handshake.",config.kafkaTopic);
        streamingClient.handshake();

        if (!streamingClient.waitFor(this.config.wait_in_ms, BayeuxClient.State.CONNECTED)) {
            log.error("kafka topic {}'s connector Not connected after waiting {}",config.kafkaTopic, this.config.wait_in_ms);
            if (authTaskCounter.decrementAndGet() == 0) {
                throw new ConnectException("not connected after max waiting time.");
            }
        } else {
            authTaskCounter.decrementAndGet();
            log.info("kafka topic {}'s connector handshake completed.",config.kafkaTopic);
        }
    }

    @Override
    public List<SourceRecord> poll() throws InterruptedException {
        List<SourceRecord> records = new ArrayList<>();
        records.add(messageQueue.take());
        return records;
    }

    @Override
    public void stop() {
        try {
            isConnectorTaskRunning = false;
            this.streamingClient.disconnect();
            this.streamingClient.abort();
            httpClient.stop();
            log.info("kafka topic {}'s connector stopped successfully.",config.kafkaTopic);
        } catch (Exception e) {
            log.error("kafka topic {}'s connector stopping failed {}.", config.kafkaTopic, e.getMessage(), e);
        }
    }

}