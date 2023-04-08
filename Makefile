echo: build
	../maelstrom/maelstrom test -w echo --bin target/debug/maelstrom_client --node-count 1 --time-limit 10 --log-stderr

generate: build
	../maelstrom/maelstrom test -w unique-ids --bin target/debug/maelstrom_client --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition --log-stderr

build:
	cargo build
