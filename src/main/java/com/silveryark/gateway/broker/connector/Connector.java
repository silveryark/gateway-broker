package com.silveryark.gateway.broker.connector;

import java.io.Closeable;

public interface Connector extends Closeable {
    void sendMore(byte[] message);

    void send(byte[] message);
}
