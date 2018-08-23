package com.silveryark.gateway.broker.controller;

import com.silveryark.gateway.broker.BrokerServer;
import com.silveryark.rpc.GenericResponse;
import com.silveryark.rpc.RPCHttpHeaders;
import com.silveryark.rpc.RPCResponse;
import com.silveryark.rpc.gateway.OutboundMessage;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;

import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

@RunWith(SpringRunner.class)
@SpringBootTest
public class MessageControllerTest {

    private static final int LENGTH_STRING = 16;

    private WebTestClient client;
    @Mock
    private BrokerServer brokerServer;

    @InjectMocks
    private MessageController messageController;

    @Before
    public void setup() {
        client = WebTestClient.bindToController(messageController).build();
    }

    @Test
    public void postCmd() {
        String requestId = RandomStringUtils.randomAlphabetic(LENGTH_STRING);
        OutboundMessage<String> message = new OutboundMessage<>(requestId,
                RandomStringUtils.randomAlphanumeric(LENGTH_STRING));
        sendMessage(BrokerServer.Topic.CMD, message);
        verify(brokerServer).apply(BrokerServer.Topic.CMD, message);
    }

    protected void sendMessage(BrokerServer.Topic topic, OutboundMessage<String> message) {
        client.post().uri("/" + topic.name().toLowerCase())
                .header(RPCHttpHeaders.REQUEST_ID, message.getRequestId())
                .accept(MediaType.APPLICATION_JSON)
                .body(BodyInserters.fromObject(message))
                .exchange()
                .expectStatus().isOk()
                .expectBody(GenericResponse.class)
                .isEqualTo(new GenericResponse(message.getRequestId(), RPCResponse.STATUS.OK, Boolean.TRUE));
        verify(brokerServer).apply(topic, message);
    }

    @Test
    public void postMessage() {
        String requestId = RandomStringUtils.randomAlphabetic(LENGTH_STRING);
        OutboundMessage<String> message = new OutboundMessage<>(requestId,
                RandomStringUtils.randomAlphanumeric(LENGTH_STRING));
        sendMessage(BrokerServer.Topic.MESSAGE, message);
        verify(brokerServer).apply(BrokerServer.Topic.MESSAGE, message);
    }

    @After
    public void after() {
        client = null;
        reset(brokerServer);
    }
}
