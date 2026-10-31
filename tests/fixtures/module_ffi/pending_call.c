#include "valkeymodule.h"

static ValkeyModuleCallArgvBlockedHandle *pending_handle;
static ValkeyModuleString *last_reply;
static long long blocked_calls;
static long long completed_calls;
extern int VM_CallArgv(ValkeyModuleCtx *, ValkeyModuleString **, int, int,
                       const ValkeyModuleReplyHandlers *, void *);
extern int VM_CallArgvAbort(ValkeyModuleCallArgvBlockedHandle *);

static void on_blocked(void *data, ValkeyModuleCtx *ctx,
                       ValkeyModuleCallArgvBlockedHandle *handle) {
    (void)data;
    (void)ctx;
    pending_handle = handle;
    blocked_calls++;
}

static int on_response(void *data, ValkeyModuleCtx *ctx, const char *reply, size_t len) {
    (void)data;
    (void)ctx;
    pending_handle = NULL;
    completed_calls++;
    if (last_reply) ValkeyModule_FreeString(NULL, last_reply);
    last_reply = ValkeyModule_CreateString(NULL, reply, len);
    ValkeyModuleCallReply *effect = ValkeyModule_Call(ctx, "SET", "!cc", "callback:effect", "done");
    if (effect) ValkeyModule_FreeCallReply(effect);
    return 0;
}

static int start_call(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc < 3) return ValkeyModule_WrongArity(ctx);
    if (pending_handle) return ValkeyModule_ReplyWithError(ctx, "ERR call is already pending");
    long long should_replicate;
    if (ValkeyModule_StringToLongLong(argv[1], &should_replicate) != VALKEYMODULE_OK)
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid propagation flag");
    ValkeyModuleReplyHandlers handlers = {0};
    handlers.version = VALKEYMODULE_REPLY_HANDLERS_VERSION;
    handlers.onBlocked = on_blocked;
    handlers.onRespAvailable = on_response;
    int flags = VALKEYMODULE_CALL_ARGV_ALLOW_BLOCK | VALKEYMODULE_CALL_ARGV_ERRORS_AS_REPLIES;
    if (should_replicate) flags |= VALKEYMODULE_CALL_ARGV_REPLICATE;
    if (VM_CallArgv(ctx, argv + 2, argc - 2, flags, &handlers, NULL) != VALKEYMODULE_OK)
        return ValkeyModule_ReplyWithError(ctx, "ERR module call failed");
    return ValkeyModule_ReplyWithSimpleString(ctx, pending_handle ? "pending" : "completed");
}

static int cancel_call(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    if (argc != 1) return ValkeyModule_WrongArity(ctx);
    int result = VM_CallArgvAbort(pending_handle);
    if (result == VALKEYMODULE_OK) pending_handle = NULL;
    return ValkeyModule_ReplyWithLongLong(ctx, result);
}

static int status_call(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    if (argc != 1) return ValkeyModule_WrongArity(ctx);
    ValkeyModule_ReplyWithArray(ctx, 3);
    ValkeyModule_ReplyWithLongLong(ctx, blocked_calls);
    ValkeyModule_ReplyWithLongLong(ctx, completed_calls);
    if (last_reply) ValkeyModule_ReplyWithString(ctx, last_reply);
    else ValkeyModule_ReplyWithNull(ctx);
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnUnload(ValkeyModuleCtx *ctx) {
    (void)ctx;
    if (last_reply) ValkeyModule_FreeString(NULL, last_reply);
    last_reply = NULL;
    return VALKEYMODULE_OK;
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    (void)argc;
    if (ValkeyModule_Init(ctx, "pending-call", 1, VALKEYMODULE_APIVER_1) != VALKEYMODULE_OK)
        return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "pending.call", start_call, "", 0, 0, 0) != VALKEYMODULE_OK ||
        ValkeyModule_CreateCommand(ctx, "pending.abort", cancel_call, "", 0, 0, 0) != VALKEYMODULE_OK ||
        ValkeyModule_CreateCommand(ctx, "pending.status", status_call, "", 0, 0, 0) != VALKEYMODULE_OK)
        return VALKEYMODULE_ERR;
    return VALKEYMODULE_OK;
}
