package kiwi;

import kiwi.store.KiwiStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Optional;

public class Kiwi {

    public static void main(String[] args) {
        KiwiStore store = KiwiStore.open(Paths.get("./db"));

        System.out.println("\n-------");
        System.out.println("KiwiDB");
        System.out.println("-------\n");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split(" ", 3);
                String command = parts[0].toUpperCase();

                switch (command) {
                    case "PUT":
                        if (parts.length != 3) {
                            System.out.println("Usage: PUT key value");
                            continue;
                        }
                        store.put(parts[1].getBytes(StandardCharsets.UTF_8), parts[2].getBytes(StandardCharsets.UTF_8));
                        break;

                    case "GET":
                        if (parts.length != 2) {
                            System.out.println("Usage: GET key");
                            continue;
                        }
                        Optional<byte[]> maybeValue = store.get(parts[1].getBytes(StandardCharsets.UTF_8));
                        if (maybeValue.isEmpty()) {
                            System.out.println("Key not found");
                            continue;
                        } else {
                            System.out.println(new String(maybeValue.get(), StandardCharsets.UTF_8));
                            break;
                        }

                    case "DEL":
                        if (parts.length != 2) {
                            System.out.println("Usage: DEL key");
                            continue;
                        }
                        store.delete(parts[1].getBytes(StandardCharsets.UTF_8));
                        break;

                    case "SIZE":
                        System.out.println(store.size());
                        break;

                    case "EXIT":
                        return;

                    default:
                        System.out.println("Unknown command. Available commands: PUT, GET, DEL, EXIT");
                        break;
                }
            }
        } catch (IOException ex) {
            System.err.println("Failed to read input: " + ex.getMessage());
        }
    }
}
