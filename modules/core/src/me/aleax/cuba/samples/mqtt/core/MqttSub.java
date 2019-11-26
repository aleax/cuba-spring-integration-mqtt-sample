package me.aleax.cuba.samples.mqtt.core;

import com.haulmont.cuba.core.global.DataManager;
import com.haulmont.cuba.core.global.Metadata;
import com.haulmont.cuba.security.app.Authentication;
import me.aleax.cuba.samples.mqtt.entity.MqttMsg;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.slf4j.Logger;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.config.EnableIntegration;
import org.springframework.integration.core.MessageSource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.Pollers;
import org.springframework.integration.endpoint.MessageProducerSupport;
import org.springframework.integration.endpoint.MethodInvokingMessageSource;
import org.springframework.integration.handler.MethodInvokingMessageHandler;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.integration.mqtt.outbound.MqttPahoMessageHandler;
import org.springframework.integration.mqtt.support.DefaultPahoMessageConverter;
import org.springframework.messaging.MessageHandler;
import org.springframework.stereotype.Component;

import javax.inject.Inject;
import java.util.Random;

@Component(MqttSub.NAME)
@EnableIntegration
@Configuration
public class MqttSub {
    public static final String NAME = "mqtt_MqttSub";

    @Inject
    private Logger log;

    @Inject
    DataManager dataManager;

    @Inject
    Authentication authentication;

    @Inject
    private Metadata metadata;

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[] { "tcp://localhost:1883" });
        options.setUserName("guest");
        options.setPassword("guest".toCharArray());
        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    public MessageProducerSupport mqttInbound() {
        MqttPahoMessageDrivenChannelAdapter adapter = new MqttPahoMessageDrivenChannelAdapter("siSampleConsumer",
                mqttClientFactory(), "#");
        adapter.setCompletionTimeout(5000);
        adapter.setConverter(new DefaultPahoMessageConverter());
        adapter.setQos(1);
        return adapter;
    }

    @Bean
    public IntegrationFlow mqttInFlow(){
        return IntegrationFlows.from(mqttInbound())
                .handle(messageHandler())
                .get();
    }

    public MethodInvokingMessageHandler messageHandler() {
        MethodInvokingMessageHandler handler = new MethodInvokingMessageHandler(this, "SaveMsg");
        return handler;
    }

    public void SaveMsg(String message) {
        log.info("val = " + message);

        authentication.begin();

        MqttMsg entity = metadata.create(MqttMsg.class);
        entity.setMsg(message);
        dataManager.commit(entity);

        authentication.end();
    }

    @Bean
    public MessageSource<?> randomIntSource() {
        MethodInvokingMessageSource source = new MethodInvokingMessageSource();
        source.setObject(new Random());
        source.setMethodName("nextInt");
        return source;
    }

    @Bean
    public IntegrationFlow someOtherFlow() {
        return IntegrationFlows.from(randomIntSource(),
                e -> e.poller(Pollers.fixedDelay(1000)))
                .transform(p -> p.toString() + " sent to MQTT")
                .handle(mqttOutbound())
                .get();
    }

    @Bean
    public MessageHandler mqttOutbound() {
        MqttPahoMessageHandler messageHandler = new MqttPahoMessageHandler("siSamplePublisher", mqttClientFactory());
        messageHandler.setAsync(true);
        messageHandler.setDefaultTopic("siSampleTopic");
        return messageHandler;
    }

}