// Minimal Valkey module ABI fixture for AGENT D D2.
//
// This intentionally avoids including valkeymodule.h. The full header resolves
// the whole API table during ValkeyModule_Init, while this milestone only
// smoke-tests the Agent D Top20 API slice.

#include <stddef.h>
#include <string.h>

#define VALKEYMODULE_OK 0
#define VALKEYMODULE_ERR 1
#define VALKEYMODULE_APIVER_1 1

#define VALKEYMODULE_READ (1 << 0)
#define VALKEYMODULE_WRITE (1 << 1)

#define VALKEYMODULE_REPLY_STRING 0
#define VALKEYMODULE_REPLY_INTEGER 2

#define VALKEYMODULE_KEYTYPE_EMPTY 0
#define VALKEYMODULE_KEYTYPE_STRING 1

typedef struct ValkeyModuleCtx ValkeyModuleCtx;
typedef struct ValkeyModuleString ValkeyModuleString;
typedef struct ValkeyModuleKey ValkeyModuleKey;
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
static void *(*ValkeyModule_GetCommand)(ValkeyModuleCtx *ctx, const char *name);
static int (*ValkeyModule_CreateSubcommand)(
    void *parent,
    const char *name,
    ValkeyModuleCmdFunc func,
    const char *strflags,
    int firstkey,
    int lastkey,
    int keystep);
static int (*ValkeyModule_ReplyWithLongLong)(ValkeyModuleCtx *ctx, long long ll);
static int (*ValkeyModule_ReplyWithSimpleString)(ValkeyModuleCtx *ctx, const char *msg);
static int (*ValkeyModule_ReplyWithString)(ValkeyModuleCtx *ctx, ValkeyModuleString *s);
static int (*ValkeyModule_ReplyWithArray)(ValkeyModuleCtx *ctx, long len);
static ValkeyModuleString *(*ValkeyModule_CreateString)(
    ValkeyModuleCtx *ctx,
    const char *ptr,
    size_t len);
static ValkeyModuleString *(*ValkeyModule_CreateStringFromLongLong)(
    ValkeyModuleCtx *ctx,
    long long ll);
static void (*ValkeyModule_FreeString)(ValkeyModuleCtx *ctx, ValkeyModuleString *s);
static const char *(*ValkeyModule_StringPtrLen)(ValkeyModuleString *s, size_t *len);
static ValkeyModuleKey *(*ValkeyModule_OpenKey)(
    ValkeyModuleCtx *ctx,
    ValkeyModuleString *keyname,
    int mode);
static void (*ValkeyModule_CloseKey)(ValkeyModuleKey *key);
static int (*ValkeyModule_KeyType)(ValkeyModuleKey *key);
static size_t (*ValkeyModule_ValueLength)(ValkeyModuleKey *key);
static int (*ValkeyModule_KeyExists)(ValkeyModuleCtx *ctx, ValkeyModuleString *keyname);
static int (*ValkeyModule_DeleteKey)(ValkeyModuleKey *key);
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
static void (*ValkeyModule_AutoMemory)(ValkeyModuleCtx *ctx);
static void *(*ValkeyModule_CreateDataType)(
    ValkeyModuleCtx *ctx,
    const char *name,
    int encver,
    void *typemethods);
static void (*ValkeyModule_Log)(ValkeyModuleCtx *ctx, const char *level, const char *fmt, ...);
static int (*ValkeyModule_GetSelectedDb)(ValkeyModuleCtx *ctx);
static int (*ValkeyModule_SelectDb)(ValkeyModuleCtx *ctx, int dbid);
static int (*ValkeyModule_Replicate)(
    ValkeyModuleCtx *ctx,
    const char *cmdname,
    const char *fmt,
    ...);
static int (*ValkeyModule_ReplicateVerbatim)(ValkeyModuleCtx *ctx);

static void *top20_type;

static int load_api(const char *name, void *target) {
    return ValkeyModule_GetApi(name, target);
}

static int init_module(ValkeyModuleCtx *ctx, const char *name, int ver, int apiver) {
    ValkeyModule_GetApi = (int (*)(const char *, void *))(*(void **)ctx);
    if (load_api("ValkeyModule_SetModuleAttribs", &ValkeyModule_SetModuleAttribs) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CreateCommand", &ValkeyModule_CreateCommand) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_GetCommand", &ValkeyModule_GetCommand) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CreateSubcommand", &ValkeyModule_CreateSubcommand) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithLongLong", &ValkeyModule_ReplyWithLongLong) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithSimpleString", &ValkeyModule_ReplyWithSimpleString) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithString", &ValkeyModule_ReplyWithString) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithArray", &ValkeyModule_ReplyWithArray) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CreateString", &ValkeyModule_CreateString) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CreateStringFromLongLong", &ValkeyModule_CreateStringFromLongLong) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_FreeString", &ValkeyModule_FreeString) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_StringPtrLen", &ValkeyModule_StringPtrLen) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_OpenKey", &ValkeyModule_OpenKey) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CloseKey", &ValkeyModule_CloseKey) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_KeyType", &ValkeyModule_KeyType) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ValueLength", &ValkeyModule_ValueLength) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_KeyExists", &ValkeyModule_KeyExists) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_DeleteKey", &ValkeyModule_DeleteKey) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_Call", &ValkeyModule_Call) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_FreeCallReply", &ValkeyModule_FreeCallReply) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CallReplyType", &ValkeyModule_CallReplyType) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CallReplyInteger", &ValkeyModule_CallReplyInteger) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CallReplyStringPtr", &ValkeyModule_CallReplyStringPtr) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_AutoMemory", &ValkeyModule_AutoMemory) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CreateDataType", &ValkeyModule_CreateDataType) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_Log", &ValkeyModule_Log) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_GetSelectedDb", &ValkeyModule_GetSelectedDb) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_SelectDb", &ValkeyModule_SelectDb) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_Replicate", &ValkeyModule_Replicate) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplicateVerbatim", &ValkeyModule_ReplicateVerbatim) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    ValkeyModule_SetModuleAttribs(ctx, name, ver, apiver);
    ValkeyModule_AutoMemory(ctx);
    ValkeyModule_Log(ctx, "notice", "top20 smoke init");
    void *type_methods[7] = {0};
    top20_type = ValkeyModule_CreateDataType(ctx, "top20dt", 0, type_methods);
    if (top20_type == NULL) return VALKEYMODULE_ERR;
    return VALKEYMODULE_OK;
}

static int top10_echo(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) {
        return ValkeyModule_ReplyWithLongLong(ctx, -1);
    }
    size_t len = 0;
    const char *ptr = ValkeyModule_StringPtrLen(argv[1], &len);
    ValkeyModuleString *copy = ValkeyModule_CreateString(ctx, ptr, len);
    int ret = ValkeyModule_ReplyWithString(ctx, copy);
    ValkeyModule_FreeString(ctx, copy);
    return ret;
}

static int top10_integer(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    if (argc != 1) {
        return ValkeyModule_ReplyWithLongLong(ctx, -1);
    }
    ValkeyModuleString *s = ValkeyModule_CreateStringFromLongLong(ctx, 42);
    size_t len = 0;
    const char *ptr = ValkeyModule_StringPtrLen(s, &len);
    int ret = (len == 2 && memcmp(ptr, "42", 2) == 0)
        ? ValkeyModule_ReplyWithLongLong(ctx, 42)
        : ValkeyModule_ReplyWithLongLong(ctx, -42);
    ValkeyModule_FreeString(ctx, s);
    return ret;
}

static int top10_keyinfo(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) {
        return ValkeyModule_ReplyWithLongLong(ctx, -1);
    }
    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, argv[1], VALKEYMODULE_READ);
    int key_type = ValkeyModule_KeyType(key);
    size_t value_len = ValkeyModule_ValueLength(key);
    ValkeyModule_CloseKey(key);
    return ValkeyModule_ReplyWithArray(ctx, 2) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, key_type) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, (long long)value_len) == VALKEYMODULE_OK
        ? VALKEYMODULE_OK
        : VALKEYMODULE_ERR;
}

static int top10_keytype(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) {
        return ValkeyModule_ReplyWithLongLong(ctx, -1);
    }
    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, argv[1], VALKEYMODULE_READ);
    int key_type = ValkeyModule_KeyType(key);
    ValkeyModule_CloseKey(key);
    return ValkeyModule_ReplyWithLongLong(ctx, key_type);
}

static int top10_call(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    if (argc != 1) {
        return ValkeyModule_ReplyWithLongLong(ctx, -1);
    }

    ValkeyModuleCallReply *incr = ValkeyModule_Call(ctx, "INCR", "c", "__module_ffi_counter__");
    int incr_type = ValkeyModule_CallReplyType(incr);
    long long incr_value = ValkeyModule_CallReplyInteger(incr);
    ValkeyModule_FreeCallReply(incr);

    ValkeyModuleCallReply *get = ValkeyModule_Call(ctx, "GET", "c", "__module_ffi_counter__");
    int get_type = ValkeyModule_CallReplyType(get);
    size_t len = 0;
    const char *ptr = ValkeyModule_CallReplyStringPtr(get, &len);
    int is_string_one = get_type == VALKEYMODULE_REPLY_STRING
        && len == 1
        && ptr != NULL
        && ptr[0] == '1';
    ValkeyModule_FreeCallReply(get);

    return ValkeyModule_ReplyWithArray(ctx, 3) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, incr_type) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, incr_value) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, is_string_one) == VALKEYMODULE_OK
        ? VALKEYMODULE_OK
        : VALKEYMODULE_ERR;
}

static int top20_db(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    if (argc != 1) {
        return ValkeyModule_ReplyWithLongLong(ctx, -1);
    }
    int initial = ValkeyModule_GetSelectedDb(ctx);
    int select_one = ValkeyModule_SelectDb(ctx, 1);
    int selected = ValkeyModule_GetSelectedDb(ctx);
    int select_back = ValkeyModule_SelectDb(ctx, initial);

    return ValkeyModule_ReplyWithArray(ctx, 4) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, initial) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, select_one) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, selected) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, select_back) == VALKEYMODULE_OK
        ? VALKEYMODULE_OK
        : VALKEYMODULE_ERR;
}

static int top20_exists_delete(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) {
        return ValkeyModule_ReplyWithLongLong(ctx, -1);
    }
    int before = ValkeyModule_KeyExists(ctx, argv[1]);
    ValkeyModuleKey *key = ValkeyModule_OpenKey(ctx, argv[1], VALKEYMODULE_WRITE);
    int del = ValkeyModule_DeleteKey(key);
    ValkeyModule_CloseKey(key);
    int after = ValkeyModule_KeyExists(ctx, argv[1]);

    return ValkeyModule_ReplyWithArray(ctx, 3) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, before) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, del) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, after) == VALKEYMODULE_OK
        ? VALKEYMODULE_OK
        : VALKEYMODULE_ERR;
}

static int top20_datatype(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    if (argc != 1) {
        return ValkeyModule_ReplyWithLongLong(ctx, -1);
    }
    return top20_type != NULL
        ? ValkeyModule_ReplyWithSimpleString(ctx, "OK")
        : ValkeyModule_ReplyWithLongLong(ctx, -1);
}

static int top30_replicate(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 3) {
        return ValkeyModule_ReplyWithLongLong(ctx, -1);
    }
    int repl = ValkeyModule_Replicate(ctx, "SET", "ss", argv[1], argv[2]);
    int verbatim = ValkeyModule_ReplicateVerbatim(ctx);
    return ValkeyModule_ReplyWithArray(ctx, 2) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, repl) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, verbatim) == VALKEYMODULE_OK
        ? VALKEYMODULE_OK
        : VALKEYMODULE_ERR;
}

static int top30_subcommand(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    if (argc != 2) {
        return ValkeyModule_ReplyWithLongLong(ctx, -1);
    }
    return ValkeyModule_ReplyWithSimpleString(ctx, "subcommand");
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    (void)argc;
    if (init_module(ctx, "top10smoke", 1, VALKEYMODULE_APIVER_1) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }
    if (ValkeyModule_CreateCommand(ctx, "top10.echo", top10_echo, "readonly fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "top10.integer", top10_integer, "readonly fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "top10.keyinfo", top10_keyinfo, "readonly fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "top10.keytype", top10_keytype, "readonly fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "top10.call", top10_call, "write fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "top20.db", top20_db, "write fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "top20.exists_delete", top20_exists_delete, "write fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "top20.datatype", top20_datatype, "readonly fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "top30.replicate", top30_replicate, "write fast", 1, 1, 1) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "top30.container", NULL, "readonly fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    void *container = ValkeyModule_GetCommand(ctx, "top30.container");
    if (container == NULL) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateSubcommand(container, "child", top30_subcommand, "readonly fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    return VALKEYMODULE_OK;
}
