#include "valkeymodule.h"

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
    return ValkeyModule_CreateCommand(
        ctx,
        "official.ping",
        official_ping,
        "readonly fast",
        0,
        0,
        0);
}
