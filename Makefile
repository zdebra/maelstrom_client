MAELSTROM_BINARY = ../maelstrom/maelstrom
BROADCAST_BINARY = target/debug/broadcast
GROW_COUNTER_BINARY = target/debug/grow_counter


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

build:
	cargo build
