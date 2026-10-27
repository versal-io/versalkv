#include "valkeymodule.h"
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static ValkeyModuleString *load_argument;
static ValkeyModuleString *notified_key;
static int callback_count;
static void (*cleanup_probe)(void);

__attribute__((destructor))
static void observe_unload(void) {
    if (cleanup_probe != NULL) cleanup_probe();
}

static int bytes_equal(ValkeyModuleString *string, const char *data, size_t len) {
    size_t actual = 0;
    const char *bytes = ValkeyModule_StringPtrLen(string, &actual);
    return bytes != NULL && actual == len && memcmp(bytes, data, len) == 0;
}

static int load_arg(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    if (load_argument == NULL) return ValkeyModule_ReplyWithError(ctx, "ERR no load argument");
    ValkeyModule_ReplyWithString(ctx, load_argument);
    ValkeyModule_FreeString(NULL, load_argument);
    load_argument = NULL;
    return VALKEYMODULE_OK;
}

static int exercise(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    ValkeyModule_AutoMemory(ctx);
    ValkeyModuleString *string = ValkeyModule_CreateString(ctx, "a\0b", 3);
    ValkeyModule_RetainString(ctx, string);
    size_t len;
    const char *data = ValkeyModule_StringPtrLen(string, &len);
    if (ValkeyModule_StringAppendBuffer(ctx, string, data, len) != VALKEYMODULE_OK ||
        !bytes_equal(string, "a\0ba\0b", 6)) return ValkeyModule_ReplyWithError(ctx, "ERR append");
    ValkeyModule_RetainString(NULL, string);
    if (ValkeyModule_StringAppendBuffer(ctx, string, "x", 1) != VALKEYMODULE_ERR)
        return ValkeyModule_ReplyWithError(ctx, "ERR shared append");
    ValkeyModule_FreeString(NULL, string);

    ValkeyModuleString *slots[] = {string, string};
    ValkeyModuleCallReply *push = ValkeyModule_Call(ctx, "RPUSH", "clbsv",
        "lifecycle:list", 7LL, "x\0y", (size_t)3, string, slots, (size_t)2);
    if (push == NULL || ValkeyModule_CallReplyType(push) != VALKEYMODULE_REPLY_INTEGER)
        return ValkeyModule_ReplyWithError(ctx, "ERR mixed argv");
    ValkeyModule_FreeCallReply(push);
    ValkeyModuleCallReply *array = ValkeyModule_Call(ctx, "LRANGE", "cll", "lifecycle:list", 0LL, -1LL);
    ValkeyModuleCallReply *child = ValkeyModule_CallReplyArrayElement(array, 1);
    ValkeyModule_FreeCallReply(child);
    ValkeyModuleString *copy = ValkeyModule_CreateStringFromCallReply(child);
    ValkeyModule_FreeCallReply(array);
    if (!bytes_equal(copy, "x\0y", 3)) return ValkeyModule_ReplyWithError(ctx, "ERR reply copy");

    ValkeyModuleString *keyname = ValkeyModule_CreateString(ctx, "lifecycle:list", 14);
    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, keyname, VALKEYMODULE_READ);
    ValkeyModuleString *item = ValkeyModule_ListGet(key, 1);
    ValkeyModule_CloseKey(key);
    if (!bytes_equal(item, "x\0y", 3)) return ValkeyModule_ReplyWithError(ctx, "ERR list output");

    ValkeyModuleCallReply *error = ValkeyModule_Call(ctx, "missing.lifecycle.command", "");
    if (error == NULL || ValkeyModule_CallReplyType(error) != VALKEYMODULE_REPLY_ERROR)
        return ValkeyModule_ReplyWithError(ctx, "ERR error root");
    if (ValkeyModule_Call(ctx, "GET", "cD", "lifecycle:key") != NULL)
        return ValkeyModule_ReplyWithError(ctx, "ERR dry run");
    if (ValkeyModule_Call(ctx, "RPUSH", "csvq", "lifecycle:list", string, slots, (size_t)2) != NULL)
        return ValkeyModule_ReplyWithError(ctx, "ERR invalid format");
    if (ValkeyModule_Replicate(ctx, "INCR", "c", "lifecycle:replica") != VALKEYMODULE_OK)
        return ValkeyModule_ReplyWithError(ctx, "ERR replicate");
    ValkeyModule_FreeString(NULL, string);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

static int notification(ValkeyModuleCtx *ctx, int type, const char *event, ValkeyModuleString *key) {
    VALKEYMODULE_NOT_USED(type);
    VALKEYMODULE_NOT_USED(event);
    if (!bytes_equal(key, "lifecycle:key", 13)) return VALKEYMODULE_OK;
    if (!ValkeyModule_IsModuleNameBusy("auto-memory-lifecycle")) return VALKEYMODULE_ERR;
    ValkeyModule_AutoMemory(ctx);
    ValkeyModule_CreateString(ctx, "callback", 8);
    if (notified_key != NULL) ValkeyModule_FreeString(NULL, notified_key);
    notified_key = key;
    ValkeyModule_RetainString(ctx, key);
    callback_count++;
    return VALKEYMODULE_OK;
}

static int notify(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    VALKEYMODULE_NOT_USED(argv);
    VALKEYMODULE_NOT_USED(argc);
    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx, "SET", "cc", "lifecycle:key", "value");
    ValkeyModule_FreeCallReply(reply);
    ValkeyModuleString *manual = ValkeyModule_CreateString(ctx, "manual", 6);
    ValkeyModule_RetainString(ctx, manual);
    int shared = ValkeyModule_StringAppendBuffer(ctx, manual, "x", 1);
    ValkeyModule_FreeString(NULL, manual);
    if (shared != VALKEYMODULE_ERR)
        return ValkeyModule_ReplyWithError(ctx, "ERR callback changed outer scope");
    ValkeyModule_FreeString(NULL, manual);
    if (callback_count != 1 || !bytes_equal(notified_key, "lifecycle:key", 13))
        return ValkeyModule_ReplyWithError(ctx, "ERR callback scope");
    ValkeyModule_FreeString(NULL, notified_key);
    notified_key = NULL;
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

static void timer_callback(ValkeyModuleCtx *ctx, void *data) {
    ValkeyModuleString *argument = data;
    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx, "SET", "cs", "lifecycle:timer", argument);
    ValkeyModule_FreeCallReply(reply);
    ValkeyModule_FreeString(NULL, argument);
}

static int schedule_timer(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    ValkeyModule_RetainString(ctx, argv[1]);
    ValkeyModule_CreateTimer(ctx, 10, timer_callback, argv[1]);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (ValkeyModule_Init(ctx, "auto-memory-lifecycle", 1, VALKEYMODULE_APIVER_1) != VALKEYMODULE_OK)
        return VALKEYMODULE_ERR;
    ValkeyModule_AutoMemory(ctx);
    ValkeyModule_CreateString(ctx, "load-one", 8);
    ValkeyModule_CreateStringFromLongLong(ctx, 42);
    ValkeyModule_CreateStringPrintf(ctx, "%s", "load-three");
    if (argc > 1) {
        const char *probe = ValkeyModule_StringPtrLen(argv[1], NULL);
        if (strncmp(probe, "probe:", 6) == 0)
            cleanup_probe = (void (*)(void))(uintptr_t)strtoull(probe + 6, NULL, 16);
    }
    if (argc > 1) return VALKEYMODULE_ERR;
    if (argc == 1) {
        load_argument = argv[0];
        ValkeyModule_RetainString(NULL, load_argument);
    }
    if (ValkeyModule_CreateCommand(ctx, "lifecycle.argv", load_arg, "readonly", 0, 0, 0) != VALKEYMODULE_OK ||
        ValkeyModule_CreateCommand(ctx, "lifecycle.exercise", exercise, "write", 0, 0, 0) != VALKEYMODULE_OK ||
        ValkeyModule_CreateCommand(ctx, "lifecycle.notify", notify, "write", 0, 0, 0) != VALKEYMODULE_OK ||
        ValkeyModule_CreateCommand(ctx, "lifecycle.timer", schedule_timer, "write", 0, 0, 0) != VALKEYMODULE_OK ||
        ValkeyModule_SubscribeToKeyspaceEvents(ctx, VALKEYMODULE_NOTIFY_STRING, notification) != VALKEYMODULE_OK)
        return VALKEYMODULE_ERR;
    return VALKEYMODULE_OK;
}
