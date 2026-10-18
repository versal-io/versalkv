// Minimal RediSearch-style module smoke fixture for AGENT D D5.
//
// The fixture keeps all state in module-local memory and uses the Module API
// only for registration, replies, context flags, logging, and memory-pressure
// hooks. It intentionally models just enough FT.* behavior for E2E smoke.

#include <stddef.h>
#include <stdlib.h>
#include <string.h>

#define VALKEYMODULE_OK 0
#define VALKEYMODULE_ERR 1
#define VALKEYMODULE_APIVER_1 1

typedef struct ValkeyModuleCtx ValkeyModuleCtx;
typedef struct ValkeyModuleString ValkeyModuleString;

typedef int (*ValkeyModuleCmdFunc)(
    ValkeyModuleCtx *ctx,
    ValkeyModuleString **argv,
    int argc);

typedef struct SearchDoc {
    char id[64];
    char title[128];
    struct SearchDoc *next;
} SearchDoc;

typedef struct SearchIndex {
    char name[64];
    SearchDoc *docs;
    struct SearchIndex *next;
} SearchIndex;

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
static int (*ValkeyModule_ReplyWithError)(ValkeyModuleCtx *ctx, const char *err);
static int (*ValkeyModule_ReplyWithStringBuffer)(ValkeyModuleCtx *ctx, const char *buf, size_t len);
static int (*ValkeyModule_ReplyWithArray)(ValkeyModuleCtx *ctx, long len);
static int (*ValkeyModule_WrongArity)(ValkeyModuleCtx *ctx);
static const char *(*ValkeyModule_StringPtrLen)(ValkeyModuleString *s, size_t *len);
static int (*ValkeyModule_GetContextFlags)(ValkeyModuleCtx *ctx);
static void (*ValkeyModule_Log)(ValkeyModuleCtx *ctx, const char *level, const char *fmt, ...);
static int (*ValkeyModule_NotifyKeyspaceEvent)(
    ValkeyModuleCtx *ctx,
    int type,
    const char *event,
    ValkeyModuleString *key);
static int (*ValkeyModule_SetContextProtectedMode)(ValkeyModuleCtx *ctx, int enabled);
static int (*ValkeyModule_FreeMemoryIfNeeded)(ValkeyModuleCtx *ctx, size_t bytes);

static SearchIndex *indexes;

static int load_api(const char *name, void *target) {
    return ValkeyModule_GetApi(name, target);
}

static int load_apis(ValkeyModuleCtx *ctx) {
    ValkeyModule_GetApi = (int (*)(const char *, void *))(*(void **)ctx);
    if (load_api("ValkeyModule_SetModuleAttribs", &ValkeyModule_SetModuleAttribs) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_CreateCommand", &ValkeyModule_CreateCommand) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithLongLong", &ValkeyModule_ReplyWithLongLong) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithSimpleString", &ValkeyModule_ReplyWithSimpleString) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithError", &ValkeyModule_ReplyWithError) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithStringBuffer", &ValkeyModule_ReplyWithStringBuffer) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_ReplyWithArray", &ValkeyModule_ReplyWithArray) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_WrongArity", &ValkeyModule_WrongArity) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_StringPtrLen", &ValkeyModule_StringPtrLen) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_GetContextFlags", &ValkeyModule_GetContextFlags) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_Log", &ValkeyModule_Log) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_NotifyKeyspaceEvent", &ValkeyModule_NotifyKeyspaceEvent) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_SetContextProtectedMode", &ValkeyModule_SetContextProtectedMode) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (load_api("ValkeyModule_FreeMemoryIfNeeded", &ValkeyModule_FreeMemoryIfNeeded) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    ValkeyModule_SetModuleAttribs(ctx, "redisearchmini", 1, VALKEYMODULE_APIVER_1);
    ValkeyModule_Log(ctx, "notice", "redisearch mini smoke init flags=%d", ValkeyModule_GetContextFlags(ctx));
    return VALKEYMODULE_OK;
}

static const char *string_ptr(ValkeyModuleString *s, size_t *len) {
    return ValkeyModule_StringPtrLen(s, len);
}

static int string_eq(ValkeyModuleString *s, const char *expected) {
    size_t len = 0;
    const char *ptr = string_ptr(s, &len);
    size_t expected_len = strlen(expected);
    return len == expected_len && memcmp(ptr, expected, len) == 0;
}

static void copy_string(ValkeyModuleString *s, char *dst, size_t dst_len) {
    size_t len = 0;
    const char *ptr = string_ptr(s, &len);
    size_t copy_len = len < dst_len - 1 ? len : dst_len - 1;
    memcpy(dst, ptr, copy_len);
    dst[copy_len] = '\0';
}

static SearchIndex *find_index(ValkeyModuleString *name) {
    size_t len = 0;
    const char *ptr = string_ptr(name, &len);
    for (SearchIndex *idx = indexes; idx != NULL; idx = idx->next) {
        if (strlen(idx->name) == len && memcmp(idx->name, ptr, len) == 0) return idx;
    }
    return NULL;
}

static long doc_count(SearchIndex *idx) {
    long count = 0;
    for (SearchDoc *doc = idx->docs; doc != NULL; doc = doc->next) count++;
    return count;
}

static int title_matches(SearchDoc *doc, ValkeyModuleString *query) {
    size_t query_len = 0;
    const char *query_ptr = string_ptr(query, &query_len);
    if (query_len == 0) return 0;
    return strstr(doc->title, query_ptr) != NULL;
}

static void free_docs(SearchDoc *doc) {
    while (doc != NULL) {
        SearchDoc *next = doc->next;
        free(doc);
        doc = next;
    }
}

static int ft_create(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 7) return ValkeyModule_WrongArity(ctx);
    if (!string_eq(argv[2], "ON") || !string_eq(argv[3], "HASH") || !string_eq(argv[4], "SCHEMA") || !string_eq(argv[6], "TEXT")) {
        return ValkeyModule_ReplyWithError(ctx, "ERR unsupported FT.CREATE syntax");
    }
    if (find_index(argv[1]) != NULL) return ValkeyModule_ReplyWithError(ctx, "ERR index already exists");

    SearchIndex *idx = (SearchIndex *)calloc(1, sizeof(SearchIndex));
    if (idx == NULL) return VALKEYMODULE_ERR;
    copy_string(argv[1], idx->name, sizeof(idx->name));
    idx->next = indexes;
    indexes = idx;
    ValkeyModule_NotifyKeyspaceEvent(ctx, 0, "ft_create", argv[1]);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

static int ft_add(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 7) return ValkeyModule_WrongArity(ctx);
    if (!string_eq(argv[4], "FIELDS") || !string_eq(argv[5], "title")) {
        return ValkeyModule_ReplyWithError(ctx, "ERR unsupported FT.ADD syntax");
    }
    SearchIndex *idx = find_index(argv[1]);
    if (idx == NULL) return ValkeyModule_ReplyWithError(ctx, "ERR no such index");
    if (ValkeyModule_FreeMemoryIfNeeded(ctx, sizeof(SearchDoc)) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;

    SearchDoc *doc = (SearchDoc *)calloc(1, sizeof(SearchDoc));
    if (doc == NULL) return VALKEYMODULE_ERR;
    copy_string(argv[2], doc->id, sizeof(doc->id));
    copy_string(argv[6], doc->title, sizeof(doc->title));
    doc->next = NULL;
    if (idx->docs == NULL) {
        idx->docs = doc;
    } else {
        SearchDoc *tail = idx->docs;
        while (tail->next != NULL) tail = tail->next;
        tail->next = doc;
    }
    ValkeyModule_NotifyKeyspaceEvent(ctx, 0, "ft_add", argv[2]);
    return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
}

static int ft_search(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 3) return ValkeyModule_WrongArity(ctx);
    SearchIndex *idx = find_index(argv[1]);
    if (idx == NULL) return ValkeyModule_ReplyWithError(ctx, "ERR no such index");
    long matches = 0;
    for (SearchDoc *doc = idx->docs; doc != NULL; doc = doc->next) {
        if (title_matches(doc, argv[2])) matches++;
    }
    if (ValkeyModule_ReplyWithArray(ctx, 1 + matches * 2) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_ReplyWithLongLong(ctx, matches) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    for (SearchDoc *doc = idx->docs; doc != NULL; doc = doc->next) {
        if (!title_matches(doc, argv[2])) continue;
        if (ValkeyModule_ReplyWithStringBuffer(ctx, doc->id, strlen(doc->id)) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
        if (ValkeyModule_ReplyWithArray(ctx, 2) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
        if (ValkeyModule_ReplyWithStringBuffer(ctx, "title", 5) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
        if (ValkeyModule_ReplyWithStringBuffer(ctx, doc->title, strlen(doc->title)) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    }
    return VALKEYMODULE_OK;
}

static int ft_info(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    SearchIndex *idx = find_index(argv[1]);
    if (idx == NULL) return ValkeyModule_ReplyWithError(ctx, "ERR no such index");
    return ValkeyModule_ReplyWithArray(ctx, 4) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithStringBuffer(ctx, "indexes", 7) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, 1) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithStringBuffer(ctx, "documents", 9) == VALKEYMODULE_OK
        && ValkeyModule_ReplyWithLongLong(ctx, doc_count(idx)) == VALKEYMODULE_OK
        ? VALKEYMODULE_OK
        : VALKEYMODULE_ERR;
}

static int ft_list(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    if (argc != 1) return ValkeyModule_WrongArity(ctx);
    long count = 0;
    for (SearchIndex *idx = indexes; idx != NULL; idx = idx->next) count++;
    if (ValkeyModule_ReplyWithArray(ctx, count) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    for (SearchIndex *idx = indexes; idx != NULL; idx = idx->next) {
        if (ValkeyModule_ReplyWithStringBuffer(ctx, idx->name, strlen(idx->name)) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    }
    return VALKEYMODULE_OK;
}

static int ft_dropindex(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    if (argc != 2) return ValkeyModule_WrongArity(ctx);
    SearchIndex **slot = &indexes;
    while (*slot != NULL) {
        SearchIndex *idx = *slot;
        size_t len = 0;
        const char *ptr = string_ptr(argv[1], &len);
        if (strlen(idx->name) == len && memcmp(idx->name, ptr, len) == 0) {
            *slot = idx->next;
            free_docs(idx->docs);
            free(idx);
            ValkeyModule_SetContextProtectedMode(ctx, 1);
            ValkeyModule_NotifyKeyspaceEvent(ctx, 0, "ft_dropindex", argv[1]);
            ValkeyModule_SetContextProtectedMode(ctx, 0);
            return ValkeyModule_ReplyWithSimpleString(ctx, "OK");
        }
        slot = &idx->next;
    }
    return ValkeyModule_ReplyWithError(ctx, "ERR no such index");
}

int ValkeyModule_OnLoad(ValkeyModuleCtx *ctx, ValkeyModuleString **argv, int argc) {
    (void)argv;
    (void)argc;
    if (load_apis(ctx) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "ft.create", ft_create, "write fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "ft.add", ft_add, "write fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "ft.search", ft_search, "readonly fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "ft.info", ft_info, "readonly fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "ft._list", ft_list, "readonly fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    if (ValkeyModule_CreateCommand(ctx, "ft.dropindex", ft_dropindex, "write fast", 0, 0, 0) != VALKEYMODULE_OK) return VALKEYMODULE_ERR;
    return VALKEYMODULE_OK;
}
