# specs/replication/Makefile
.PHONY: check clean

CHECKER := p

check: ReplicationHandshake.p ReplPeer.p
	$(CHECKER) check ReplicationHandshake.p

clean:
	rm -rf PGenerated/