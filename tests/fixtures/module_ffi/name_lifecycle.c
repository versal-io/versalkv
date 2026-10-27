#include "valkeymodule.h"
#include <stdio.h>

static int reply_ok(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (ValkeyModule_Init(ctx, "name-lifecycle", 1, VALKEYMODULE_APIVER_1)
            == VALKEYMODULE_ERR || argc < 1) {
        return VALKEYMODULE_ERR;
    }
    char run[128], root[128];
    const char *prefix = ValkeyModule_StringPtrLen(argv[0], NULL);
    snprintf(run, sizeof(run), "%s.Run", prefix);
    snprintf(root, sizeof(root), "%s.Root", prefix);
    if (ValkeyModule_CreateCommand(ctx, run, reply_ok, "readonly", 0, 0, 0)
            == VALKEYMODULE_ERR ||
        ValkeyModule_CreateCommand(ctx, root, NULL, "", 0, 0, 0)
            == VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    ValkeyModuleCommand *parent = ValkeyModule_GetCommand(ctx, root);
    if (ValkeyModule_CreateSubcommand(parent, "Leaf", reply_ok, "readonly", 0, 0, 0)
            == VALKEYMODULE_ERR ||
        ValkeyModule_CreateSubcommand(parent, "Leaf", reply_ok, "readonly", 0, 0, 0)
            != VALKEYMODULE_ERR) {
        return VALKEYMODULE_ERR;
    }
    ValkeyModuleCallReply *capture = ValkeyModule_Call(ctx, "name.capture", "s", argv[0]);
    if (capture == NULL) return VALKEYMODULE_ERR;
    ValkeyModule_FreeCallReply(capture);
    ValkeyModuleCallReply *nested = ValkeyModule_Call(ctx, run, "");
    if (nested == NULL) return VALKEYMODULE_ERR;
    ValkeyModule_FreeCallReply(nested);
    return argc > 1 ? VALKEYMODULE_ERR : VALKEYMODULE_OK;
}
