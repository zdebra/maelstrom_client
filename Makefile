MAELSTROM_BINARY = ./maelstrom/maelstrom
BROADCAST_BINARY = target/debug/broadcast
GROW_COUNTER_BINARY = target/debug/grow_counter
KAFKA_BINARY = target/debug/kafka
KV_STORE_BINARY = target/debug/kv-store


echo: build
	$(MAELSTROM_BINARY) test -w echo --bin $(BROADCAST_BINARY) --node-count 1 --time-limit 10 --log-stderr

generate: build
	$(MAELSTROM_BINARY) test -w unique-ids --bin $(BROADCAST_BINARY) --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition --log-stderr

broadcast: build
	$(MAELSTROM_BINARY) test -w broadcast --bin $(BROADCAST_BINARY) --node-count 1 --time-limit 20 --rate 10 --log-stderr

broadcast2: build
	$(MAELSTROM_BINARY) test -w broadcast --bin $(BROADCAST_BINARY) --node-count 5 --time-limit 20 --rate 10 --log-stderr


echo2: build
	$(MAELSTROM_BINARY) test -w echo --bin $(GROW_COUNTER_BINARY) --node-count 1 --time-limit 10 --log-stderr

counter: build
	$(MAELSTROM_BINARY) test -w g-counter --bin $(GROW_COUNTER_BINARY) --node-count 3 --rate 100 --time-limit 20 --nemesis partition --log-stderr

echo_kafka: build
	$(MAELSTROM_BINARY) test -w echo --bin $(KAFKA_BINARY) --node-count 1 --time-limit 10 --log-stderr

kafka: build
	$(MAELSTROM_BINARY) test -w kafka --bin $(KAFKA_BINARY) --node-count 1 --concurrency 2n --time-limit 20 --rate 1000 --log-stderr

kafka_multi: build
	$(MAELSTROM_BINARY) test -w kafka --bin $(KAFKA_BINARY) --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --log-stderr

kv_store: build
	$(MAELSTROM_BINARY) test -w txn-rw-register --bin $(KV_STORE_BINARY) --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total --log-stderr

kv_store_multi_node: build
	$(MAELSTROM_BINARY) test -w txn-rw-register --bin $(KV_STORE_BINARY) --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --log-stderr
build:
	cargo build
