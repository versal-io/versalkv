// Minimal RedisJSON-style module smoke fixture for AGENT D D2.
//
// It is intentionally small: commands use ordinary Valkey keys internally and
// exercise the module ABI through command registration, replies, String APIs,
// and ValkeyModule_Call round trips.

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define VALKEYMODULE_OK 0
#define VALKEYMODULE_ERR 1
#define VALKEYMODULE_APIVER_1 1

#define VALKEYMODULE_REPLY_STRING 0
#define VALKEYMODULE_REPLY_INTEGER 2

typedef struct ValkeyModuleCtx ValkeyModuleCtx;
typedef struct ValkeyModuleString ValkeyModuleString;
typedef struct ValkeyModuleCallReply ValkeyModuleCallReply;

typedef int (*ValkeyModuleCmdFunc)(
    ValkeyModuleCtx *ctx,
    ValkeyModuleString **argv,
    int argc);

static int (*ValkeyModule_GetApi)(const char *name, void *ptr);
static void (*ValkeyModule_SetModuleAttribs)(
    ValkeyModuleCtx *ctx,
    const char *name,
    int ver,
    int apiver);
static int (*ValkeyModule_CreateCommand)(
    ValkeyModuleCtx *ctx,
    const char *name,
    ValkeyModuleCmdFunc func,
    const char *strflags,
    int firstkey,
    int lastkey,
    int keystep);
static int (*ValkeyModule_ReplyWithLongLong)(ValkeyModuleCtx *ctx, long long ll);
static int (*ValkeyModule_ReplyWithSimpleString)(ValkeyModuleCtx *ctx, const char *msg);
static int (*ValkeyModule_ReplyWithStringBuffer)(ValkeyModuleCtx *ctx, const char *buf, size_t len);
static int (*ValkeyModule_ReplyWithArray)(ValkeyModuleCtx *ctx, long len);
static int (*ValkeyModule_WrongArity)(ValkeyModuleCtx *ctx);
static const char *(*ValkeyModule_StringPtrLen)(ValkeyModuleString *s, size_t *len);
static ValkeyModuleString *(*ValkeyModule_CreateString)(
    ValkeyModuleCtx *ctx,
    const char *ptr,
    size_t len);
static void (*ValkeyModule_FreeString)(ValkeyModuleCtx *ctx, ValkeyModuleString *s);
static ValkeyModuleCallReply *(*ValkeyModule_Call)(
    ValkeyModuleCtx *ctx,
    const char *cmdname,
    const char *fmt,
    ...);
static void (*ValkeyModule_FreeCallReply)(ValkeyModuleCallReply *reply);
static int (*ValkeyModule_CallReplyType)(ValkeyModuleCallReply *reply);
static long long (*ValkeyModule_CallReplyInteger)(ValkeyModuleCallReply *reply);
static const char *(*ValkeyModule_CallReplyStringPtr)(
    ValkeyModuleCallReply *reply,
    size_t *len);

static int load_api(const char *name, void *target) {
    return ValkeyModule_GetApi(name, target);
}

static int load_apis(ValkeyModuleCtx *ctx) {
    ValkeyModule_GetApi = (int (*)(const char *, void *))(*(void **)ctx);
    if (load_api("ValkeyModule_SetModuleAttribs", &ValkeyModule_SetModuleAttribs) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CreateCommand", &ValkeyModule_CreateCommand) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithLongLong", &ValkeyModule_ReplyWithLongLong) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithSimpleString", &ValkeyModule_ReplyWithSimpleString) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithStringBuffer", &ValkeyModule_ReplyWithStringBuffer) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithArray", &ValkeyModule_ReplyWithArray) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_WrongArity", &ValkeyModule_WrongArity) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_StringPtrLen", &ValkeyModule_StringPtrLen) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CreateString", &ValkeyModule_CreateString) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_FreeString", &ValkeyModule_FreeString) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_Call", &ValkeyModule_Call) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_FreeCallReply", &ValkeyModule_FreeCallReply) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CallReplyType", &ValkeyModule_CallReplyType) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CallReplyInteger", &ValkeyModule_CallReplyInteger) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CallReplyStringPtr", &ValkeyModule_CallReplyStringPtr) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    ValkeyModule_SetModuleAttribs(ctx, "redisjsonsmoke", 1, VALKEYMODULE_APIVER_1);
    return VALKEYMODULE_OK;
}

static int call_set(ValkeyModuleCtx *ctx, ValkeyModuleString *key, const char *value, size_t len) {
    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx, "SET", "sb", key, value, len);
    if (reply == NULL) return VALKEYMODULE_ERR;
    ValkeyModule_FreeCallReply(reply);
    return VALKEYMODULE_OK;
}

static char *call_get_dup(ValkeyModuleCtx *ctx, ValkeyModuleString *key, size_t *len) {
    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx, "GET", "s", key);
    if (reply == NULL) return NULL;
    const char *ptr = ValkeyModule_CallReplyStringPtr(reply, len);
    if (ptr == NULL) {
        ValkeyModule_FreeCallReply(reply);
        return NULL;
    }
    char *out = (char *)malloc(*len + 1);
    if (out != NULL) {
        memcpy(out, ptr, *len);
        out[*len] = '\0';
    }
    ValkeyModule_FreeCallReply(reply);
    return out;
}

static int json_set(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 4) return ValkeyModule_WrongArity(ctx);
    size_t len = 0;
    const char *json = ValkeyModule_StringPtrLen(argv[3], &len);
    if (call_set(ctx, argv[1], json, len) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

static int json_get(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    size_t len = 0;
    char *json = call_get_dup(ctx, argv[1], &len);
    if (json == NULL) return ValkeyModule_ReplyWithStringBuffer(ctx, "", 0);
    int ret = ValkeyModule_ReplyWithStringBuffer(ctx, json, len);
    free(json);
    return ret;
}

static int json_type(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    size_t len = 0;
    char *json = call_get_dup(ctx, argv[1], &len);
    if (json == NULL || len == 0) {
        free(json);
        return ValkeyModule_ReplyWithSimpleString(ctx, "null");
    }
    const char *typ = "string";
    if (json[0] == '{') typ = "object";
    else if (json[0] == '[') typ = "array";
    else if ((json[0] >= '0' && json[0] <= '9') || json[0] == '-') typ = "number";
    int ret = ValkeyModule_ReplyWithSimpleString(ctx, typ);
    free(json);
    return ret;
}

static int json_objlen(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    size_t len = 0;
    char *json = call_get_dup(ctx, argv[1], &len);
    long long count = (json != NULL && len >= 2 && json[0] == '{') ? 1 : 0;
    free(json);
    return ValkeyModule_ReplyWithLongLong(ctx, count);
}

static int json_arrappend(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 4) return ValkeyModule_WrongArity(ctx);
    size_t old_len = 0;
    char *old = call_get_dup(ctx, argv[1], &old_len);
    size_t item_len = 0;
    const char *item = ValkeyModule_StringPtrLen(argv[3], &item_len);
    if (old == NULL || old_len < 2) {
        free(old);
        return VALKEYMODULE_ERR;
    }
    size_t new_len = old_len + item_len + 1;
    char *next = (char *)malloc(new_len + 1);
    if (next == NULL) {
        free(old);
        return VALKEYMODULE_ERR;
    }
    memcpy(next, old, old_len - 1);
    next[old_len - 1] = ',';
    memcpy(next + old_len, item, item_len);
    next[new_len - 1] = ']';
    next[new_len] = '\0';
    int commas = 0;
    for (size_t i = 0; i < new_len; i++) {
        if (next[i] == ',') commas++;
    }
    if (call_set(ctx, argv[1], next, new_len) != VALKEYMODULE_OK) {
        free(old);
        free(next);
        return VALKEYMODULE_ERR;
    }
    free(old);
    free(next);
    return ValkeyModule_ReplyWithLongLong(ctx, commas + 1);
}

static int json_numincrby(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 4) return ValkeyModule_WrongArity(ctx);
    size_t old_len = 0;
    char *old = call_get_dup(ctx, argv[1], &old_len);
    size_t inc_len = 0;
    const char *inc = ValkeyModule_StringPtrLen(argv[3], &inc_len);
    long current = old == NULL ? 0 : strtol(old, NULL, 10);
    char inc_buf[64];
    size_t copy_len = inc_len < sizeof(inc_buf) - 1 ? inc_len : sizeof(inc_buf) - 1;
    memcpy(inc_buf, inc, copy_len);
    inc_buf[copy_len] = '\0';
    current += strtol(inc_buf, NULL, 10);
    char out[64];
    int n = snprintf(out, sizeof(out), "%ld", current);
    int ret = call_set(ctx, argv[1], out, (size_t)n) == VALKEYMODULE_OK
        ? ValkeyModule_ReplyWithStringBuffer(ctx, out, (size_t)n)
        : VALKEYMODULE_ERR;
    free(old);
    return ret;
}

static int json_mget(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc < 3) return ValkeyModule_WrongArity(ctx);
    if (ValkeyModule_ReplyWithArray(ctx, argc - 2) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    for (int i = 1; i < argc - 1; i++) {
        size_t len = 0;
        char *json = call_get_dup(ctx, argv[i], &len);
        if (json == NULL) return VALKEYMODULE_ERR;
        if (ValkeyModule_ReplyWithStringBuffer(ctx, json, len) != VALKEYMODULE_OK) {
            free(json);
            return VALKEYMODULE_ERR;
        }
        free(json);
    }
    return VALKEYMODULE_OK;
}

static int json_resp(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    size_t len = 0;
    char *json = call_get_dup(ctx, argv[1], &len);
    if (json == NULL) return VALKEYMODULE_ERR;
    const char *typ = "string";
    if (len > 0 && ((json[0] >= '0' && json[0] <= '9') || json[0] == '-')) typ = "number";
    int ret = ValkeyModule_ReplyWithArray(ctx, 2) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithStringBuffer(ctx, typ, strlen(typ)) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithStringBuffer(ctx, json, len) == VALKEYMODULE_OK
        ? VALKEYMODULE_OK
        : VALKEYMODULE_ERR;
    free(json);
    return ret;
}

static int json_clear(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    return call_set(ctx, argv[1], "{}", 2) == VALKEYMODULE_OK
        ? ValkeyModule_ReplyWithLongLong(ctx, 1)
        : VALKEYMODULE_ERR;
}

static int json_del(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    ValkeyModuleCallReply *reply = ValkeyModule_Call(ctx, "DEL", "s", argv[1]);
    if (reply == NULL) return VALKEYMODULE_ERR;
    long long deleted = ValkeyModule_CallReplyInteger(reply);
    ValkeyModule_FreeCallReply(reply);
    return ValkeyModule_ReplyWithLongLong(ctx, deleted);
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    (void)argc;
    if (load_apis(ctx) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "json.set", json_set, "write fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "json.get", json_get, "readonly fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "json.type", json_type, "readonly fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "json.objlen", json_objlen, "readonly fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "json.arrappend", json_arrappend, "write fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "json.numincrby", json_numincrby, "write fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "json.mget", json_mget, "readonly fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "json.resp", json_resp, "readonly fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "json.clear", json_clear, "write fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "json.del", json_del, "write fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    return VALKEYMODULE_OK;
}
