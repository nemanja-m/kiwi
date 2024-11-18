# KiWi

KiWi is a simple key-value store that supports the following operations:

- `PUT <key> <value>`: Store the given value under the given key.
- `GET <key>`: Retrieve the value stored under the given key.
- `DELETE <key>`: Remove the value stored under the given key.

## Build

To build the project, run the following command:

```shell
./gradlew build
```

## Test

To execute the tests, use the following command:

```shell
./gradlew test
```

## Run

To run the project, execute the following commands:

```shell
./gradlew assembleDist installDist
```

```shell
./build/install/kiwi/bin/kiwi
```
