package com.silveryark.gateway.broker;

import com.silveryark.gateway.broker.connector.Connector;
import com.silveryark.rpc.gateway.OutboundMessage;
import com.silveryark.rpc.serializer.OutboundMessageSerializer;
import org.apache.commons.lang3.RandomStringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BrokerServerTest {

    @Autowired
    private OutboundMessageSerializer serializer;

    @Mock
    private Connector connector;

    private BrokerServer brokerServer;

    @Before
    public void init() {
        brokerServer = new BrokerServer(serializer, connector);
    }

    @Test
    public void addCmdMessage() throws IOException {
        OutboundMessage<String> outboundMessage = new OutboundMessage<>(RandomStringUtils.randomAlphabetic(16),
                RandomStringUtils.randomAlphabetic(16));
        brokerServer.apply(BrokerServer.Topic.CMD, outboundMessage);
        verify(connector, timeout(200)).sendMore(BrokerServer.Topic.CMD.name().getBytes());
        verify(connector, timeout(200)).send(serializer.serialize(outboundMessage));
    }

    @Test
    public void addMessageMessage() throws IOException {
        OutboundMessage<String> outboundMessage = new OutboundMessage<>(RandomStringUtils.randomAlphabetic(16),
                RandomStringUtils.randomAlphabetic(16));
        brokerServer.apply(BrokerServer.Topic.MESSAGE, outboundMessage);
        verify(connector, timeout(200)).sendMore(BrokerServer.Topic.MESSAGE.name().getBytes());
        verify(connector, timeout(200)).send(serializer.serialize(outboundMessage));
    }

    @After
    public void after() {
        reset(connector);
        brokerServer.destroy();
        brokerServer = null;
    }
}
