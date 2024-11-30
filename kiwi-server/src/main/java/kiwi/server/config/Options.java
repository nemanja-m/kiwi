package kiwi.server.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import kiwi.server.resp.config.ServerConfig;

public class Options {

    public static final Options defaults = new Options();

    public final ServerConfig server;

    public Options() {
        Config config = ConfigFactory.load();
        this.server = new ServerConfig(config.getConfig("kiwi.server"));
    }
}
