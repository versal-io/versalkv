#include "valkeymodule.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char module_name[64];
static char command_name[96];
static int should_refuse;
static void *private_buffer;
static void (*probe)(int);

__attribute__((destructor))
static void library_closed(void) {
    if (probe != NULL) probe(private_buffer == NULL ? 3 : 4);
    free(private_buffer);
    private_buffer = NULL;
}

static int ping(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    ValkeyModule_AutoMemory(ctx);
    ValkeyModule_CreateString(ctx, "nested", 6);
    ValkeyModule_SetModuleOptions(ctx, 3);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

static void timer(ValkeyModuleCtx *ctx, void *data) {
    VALKEYMODULE_NOT_USED(ctx);
    VALKEYMODULE_NOT_USED(data);
}

#ifdef LEGACY_LOADER
#define LOADER_ONLOAD RedisModule_OnLoad
#define LOADER_ONUNLOAD RedisModule_OnUnload
#else
#define LOADER_ONLOAD ValkeyModule_OnLoad
#define LOADER_ONUNLOAD ValkeyModule_OnUnload
#endif

int LOADER_ONLOAD(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc < 2) return VALKEYMODULE_ERR;
    ValkeyModule_GetApi = (int (*)(const char *, void *))(uintptr_t)((void **)ctx)[0];
    if (ValkeyModule_GetApi == NULL ||
        ValkeyModule_GetApi("ValkeyModule_StringPtrLen", &ValkeyModule_StringPtrLen) != VALKEYMODULE_OK)
        return VALKEYMODULE_ERR;
    snprintf(module_name, sizeof(module_name), "%s", ValkeyModule_StringPtrLen(argv[0], NULL));
    if (ValkeyModule_Init(ctx, module_name, 1, VALKEYMODULE_APIVER_1) != VALKEYMODULE_OK)
        return VALKEYMODULE_ERR;
    /* Return a safe error if the host has not implemented this required API. */
    if (ValkeyModule_SetModuleOptions == NULL) return VALKEYMODULE_ERR;
    ValkeyModule_SetModuleOptions(ctx, 1);
    ValkeyModule_SetModuleOptions(ctx, 2);
    snprintf(command_name, sizeof(command_name), "%s.ping", module_name);
    if (ValkeyModule_CreateCommand(ctx, command_name, ping, "readonly", 0, 0, 0) != VALKEYMODULE_OK)
        return VALKEYMODULE_ERR;
    if (argc > 2)
        probe = (void (*)(int))(uintptr_t)strtoull(ValkeyModule_StringPtrLen(argv[2], NULL), NULL, 16);
    const char *mode = ValkeyModule_StringPtrLen(argv[1], NULL);
    should_refuse = strcmp(mode, "refuse") == 0;
    if (strcmp(mode, "fail") == 0) return VALKEYMODULE_ERR;
    if (strcmp(mode, "type") == 0) {
        ValkeyModuleTypeMethods methods = {.version = VALKEYMODULE_TYPE_METHOD_VERSION};
        if (ValkeyModule_CreateDataType(ctx, "loaderTyp", 0, &methods) == NULL)
            return VALKEYMODULE_ERR;
    }
    if (strcmp(mode, "timer") == 0)
        ValkeyModule_CreateTimer(ctx, 600000, timer, NULL);
    private_buffer = malloc(32);
    return private_buffer == NULL ? VALKEYMODULE_ERR : VALKEYMODULE_OK;
}

#ifndef NO_LOADER_UNLOAD
int LOADER_ONUNLOAD(ValkeyModuleCtx *ctx) {
    if (probe != NULL) probe(1);
    if (ctx == NULL || !ValkeyModule_IsModuleNameBusy(module_name) ||
        ValkeyModule_GetSelectedDb(ctx) != 0)
        return VALKEYMODULE_ERR;
    int (*get_api)(const char *, void *) = (int (*)(const char *, void *))(uintptr_t)((void **)ctx)[0];
    void *resolved = NULL;
    if (get_api == NULL || get_api("ValkeyModule_SetModuleOptions", &resolved) != VALKEYMODULE_OK ||
        resolved == NULL)
        return VALKEYMODULE_ERR;
    ValkeyModule_SetModuleOptions(ctx, 4);
    ValkeyModule_AutoMemory(ctx);
    ValkeyModule_CreateString(ctx, "unload", 6);
    ValkeyModuleString *keyname = ValkeyModule_CreateString(ctx, "loader:key", 10);
    if (ValkeyModule_OpenKey(ctx, keyname, VALKEYMODULE_WRITE) == NULL)
        return VALKEYMODULE_ERR;
    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx, command_name, "");
    if (reply == NULL || ValkeyModule_CallReplyType(reply) != VALKEYMODULE_REPLY_STRING)
        return VALKEYMODULE_ERR;
    ValkeyModule_SetModuleOptions(ctx, 4);
    if (probe != NULL) probe(2);
    if (should_refuse) return VALKEYMODULE_ERR;
    free(private_buffer);
    private_buffer = NULL;
    return VALKEYMODULE_OK;
}
#endif
