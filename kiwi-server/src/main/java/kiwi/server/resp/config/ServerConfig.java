package kiwi.server.resp.config;

import com.typesafe.config.Config;

public class ServerConfig {
    public final String host;
    public final int port;

    public ServerConfig(Config config) {
        this.host = config.getString("host");
        this.port = config.getInt("port");
    }
}
