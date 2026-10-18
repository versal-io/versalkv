// specs/replication/ReplicationHandshake.p
using Replication;

machine ReplicationHandshake {
    var peer: ReplPeer;

    start state Init {
        entry (peer: ReplPeer) {
            this.peer = peer;
            print("Connecting: TCP connection established");
        }
        on ePing goto WaitPong;
        on eTimeout -> Init;
    }

    state WaitPong {
        entry { print("ReceivePong: waiting for +PONG"); }
        on ePong goto SendReplconfPort;
        on eErrorReply -> assert(false, "unexpected error in WaitPong");
        on eTimeout -> assert(false, "timeout in WaitPong: should disconnect");
    }

    state SendReplconfPort {
        entry { print("ReceivePortReply: waiting for REPLCONF port +OK"); }
        on eOkReply goto SendReplconfCapa;
        on eErrorReply -> assert(false, "REPLCONF port rejected");
        on eTimeout -> assert(false, "timeout in SendReplconfPort");
    }

    state SendReplconfCapa {
        entry { print("ReceiveCapaReply: waiting for REPLCONF capa +OK"); }
        on eOkReply goto SendPsync;
        on eErrorReply -> assert(false, "REPLCONF capa rejected");
        on eTimeout -> assert(false, "timeout in SendReplconfCapa");
    }

    state SendPsync {
        entry { print("ReceivePsyncReply: waiting for +FULLRESYNC"); }
        on eFullResync goto TransferRdb;
        on eErrorReply -> assert(false, "PSYNC rejected");
        on eTimeout -> assert(false, "timeout in SendPsync");
    }

    state TransferRdb {
        entry { print("Transfer: receiving RDB bulk"); }
        on eRdbComplete goto Connected;
        on eRdbError -> assert(false, "RDB transfer failed");
        on eTimeout -> assert(false, "timeout during RDB transfer");
    }

    state Connected {
        entry { print("Connected: replication handshake complete"); }
    }
}
