#include "valkeymodule.h"

extern int VM_CallArgv(
    ValkeyModuleCtx *ctx,
    ValkeyModuleString **argv,
    int argc,
    int flags,
    const ValkeyModuleReplyHandlers *handlers,
    void *reply_ctx);
extern int VM_ReplyRaw(ValkeyModuleCtx *ctx, const char *proto, size_t proto_len);

static int forward_raw_reply(
    void *reply_ctx,
    ValkeyModuleCtx *ctx,
    const char *proto,
    size_t proto_len) {
    *(int *)reply_ctx = VM_ReplyRaw(ctx, proto, proto_len);
    return 0;
}

static int official_call_argv(
    ValkeyModuleCtx *ctx,
    ValkeyModuleString **argv,
    int argc) {
    if (argc < 2) {
        return ValkeyModule_WrongArity(ctx);
    }
    const ValkeyModuleReplyHandlers handlers = {
        .version = VALKEYMODULE_REPLY_HANDLERS_VERSION,
        .onRespAvailable = forward_raw_reply
    };
    int reply_status = VALKEYMODULE_ERR;
    if (VM_CallArgv(ctx, argv + 1, argc - 1, 0, &handlers, &reply_status)
        != VALKEYMODULE_OK) {
        return ValkeyModule_ReplyWithError(ctx, "ERR direct argv call failed");
    }
    return reply_status;
}

static int official_ping(
    ValkeyModuleCtx *ctx,
    ValkeyModuleString **argv,
    int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    return ValkeyModule_ReplyWithSimpleString(ctx, "PONG");
}

int ValkeyModule_OnLoad(
    ValkeyModuleCtx *ctx,
    ValkeyModuleString **argv,
    int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    if (ValkeyModule_Init(ctx, "officialheadersmoke", 1, VALKEYMODULE_APIVER_1)
        == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateCommand(
        ctx,
        "official.ping",
        official_ping,
        "readonly fast",
        0,
        0,
        0) == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    return ValkeyModule_CreateCommand(
        ctx, "official.callargv", official_call_argv, "write", 0, 0, 0);
}
