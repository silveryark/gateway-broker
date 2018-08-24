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
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;

import static org.mockito.Mockito.*;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BrokerServerExceptionTest {

    @Mock
    private OutboundMessageSerializer serializer;

    @Mock
    private Connector connector;

    private BrokerServer brokerServer;

    @Before
    public void init() {
        brokerServer = new BrokerServer(serializer, connector);
    }

    @Test
    public void testIOException() throws IOException {
        when(serializer.serialize(any())).thenThrow(IOException.class);
        OutboundMessage<String> outboundMessage = new OutboundMessage<>(RandomStringUtils.randomAlphabetic(16),
                RandomStringUtils.randomAlphabetic(16));
        brokerServer.apply(BrokerServer.Topic.CMD, outboundMessage);
        verify(connector, after(200).never()).sendMore(any());
        verify(connector, after(200).never()).send(any());
    }

    @Test
    public void testOtherException() throws IOException {
        when(serializer.serialize(any())).thenThrow(NullPointerException.class);
        OutboundMessage<String> outboundMessage = new OutboundMessage<>(RandomStringUtils.randomAlphabetic(16),
                RandomStringUtils.randomAlphabetic(16));
        brokerServer.apply(BrokerServer.Topic.CMD, outboundMessage);
        verify(connector, after(200).never()).sendMore(any());
        verify(connector, after(200).never()).send(any());
    }

    @After
    public void afterTest() {
        reset(connector);
        brokerServer.destroy();
        brokerServer = null;
    }
}
