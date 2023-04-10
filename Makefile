MAELSTROM_BINARY = ../maelstrom/maelstrom
CLIENT_BINARY = target/debug/maelstrom_client


echo: build
	$(MAELSTROM_BINARY) test -w echo --bin $(CLIENT_BINARY) --node-count 1 --time-limit 10 --log-stderr

generate: build
	$(MAELSTROM_BINARY) test -w unique-ids --bin $(CLIENT_BINARY) --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition --log-stderr

broadcast: build
	$(MAELSTROM_BINARY) test -w broadcast --bin $(CLIENT_BINARY) --node-count 1 --time-limit 20 --rate 10 --log-stderr

broadcast2: build
	$(MAELSTROM_BINARY) test -w broadcast --bin $(CLIENT_BINARY) --node-count 5 --time-limit 20 --rate 10 --log-stderr


build:
	cargo build
