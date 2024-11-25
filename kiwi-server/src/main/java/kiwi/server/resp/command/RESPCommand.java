package kiwi.server.resp.command;

import java.util.List;

public record RESPCommand(CommandType commandType, List<String> arguments) {
}
