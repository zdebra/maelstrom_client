run:
	cargo build
	../maelstrom/maelstrom test -w echo --bin target/debug/maelstrom_client --node-count 1 --time-limit 10 --log-stderr
