package kiwi;

import kiwi.common.Bytes;
import kiwi.storage.KiwiStore;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
                        store.put(Bytes.wrap(parts[1]), Bytes.wrap(parts[2]));
                        break;

                    case "GET":
                        if (parts.length != 2) {
                            System.out.println("Usage: GET key");
                            continue;
                        }
                        Optional<Bytes> maybeValue = store.get(Bytes.wrap(parts[1]));
                        if (maybeValue.isEmpty()) {
                            System.out.println("Key not found");
                            continue;
                        } else {
                            System.out.println(maybeValue.get());
                            break;
                        }

                    case "DEL":
                        if (parts.length != 2) {
                            System.out.println("Usage: DEL key");
                            continue;
                        }
                        store.delete(Bytes.wrap(parts[1]));
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
