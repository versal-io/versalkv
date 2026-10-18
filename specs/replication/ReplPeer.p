// specs/replication/ReplPeer.p
namespace Replication;

event ePing;
event ePong;
event eOkReply;
event eFullResync : (replid: string, offset: int);
event eRdbData : (len: int, data: seq(uint8));
event eRdbComplete;
event eRdbError;
event eTimeout;
event eErrorReply : (msg: string);

machine ReplPeer {
    var connected: bool;

    start state Idle {
        entry { connected = true; }
        on ePing goto Idle;
        on ePong goto Idle;
        on eOkReply goto Idle;
        on eFullResync goto Idle;
        on eRdbData goto Idle;
        on eRdbComplete goto Idle;
        on eRdbError goto Idle;
        on eTimeout goto Idle;
        on eErrorReply goto Idle;
    }
}
