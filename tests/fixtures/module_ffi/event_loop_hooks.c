#include <stdint.h>

#define VALKEYMODULE_OK 0
#define VALKEYMODULE_ERR 1
#define VALKEYMODULE_APIVER_1 1
#define VALKEYMODULE_EVENT_EVENTLOOP 15
#define VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP 0
#define VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP 1

typedef struct ValkeyModuleCtx ValkeyModuleCtx;
typedef struct ValkeyModuleString ValkeyModuleString;
typedef struct ValkeyModuleEvent {
    uint64_t id;
    uint64_t dataver;
} ValkeyModuleEvent;
typedef int (*ValkeyModuleCmdFunc)(ValkeyModuleCtx *, ValkeyModuleString **, int);
typedef void (*ValkeyModuleEventCallback)(
    ValkeyModuleCtx *, ValkeyModuleEvent, uint64_t, void *);

static int (*ValkeyModule_GetApi)(const char *name, void *target);
static void (*ValkeyModule_SetModuleAttribs)(
    ValkeyModuleCtx *ctx, const char *name, int ver, int apiver);
static int (*ValkeyModule_CreateCommand)(
    ValkeyModuleCtx *ctx,
    const char *name,
    ValkeyModuleCmdFunc callback,
    const char *flags,
    int firstkey,
    int lastkey,
    int keystep);
static int (*ValkeyModule_ReplyWithArray)(ValkeyModuleCtx *ctx, long length);
static int (*ValkeyModule_ReplyWithLongLong)(ValkeyModuleCtx *ctx, long long value);
static int (*ValkeyModule_SubscribeToServerEvent)(
    ValkeyModuleCtx *ctx, ValkeyModuleEvent event, ValkeyModuleEventCallback callback);

static long long before_sleep_count;
static long long after_sleep_count;

static int load_api(const char *name, void *target) {
    return ValkeyModule_GetApi(name, target);
}

static void event_loop_callback(
    ValkeyModuleCtx *ctx,
    ValkeyModuleEvent event,
    uint64_t subevent,
    void *data) {
    (void)ctx;
    (void)data;
    if (event.id != VALKEYMODULE_EVENT_EVENTLOOP) return;
    if (subevent == VALKEYMODULE_SUBEVENT_EVENTLOOP_BEFORE_SLEEP) {
        before_sleep_count++;
    } else if (subevent == VALKEYMODULE_SUBEVENT_EVENTLOOP_AFTER_SLEEP) {
        after_sleep_count++;
    }
}

static int event_loop_counts(
    ValkeyModuleCtx *ctx,
    ValkeyModuleString **argv,
    int argc) {
    (void)argv;
    (void)argc;
    ValkeyModule_ReplyWithArray(ctx, 2);
    ValkeyModule_ReplyWithLongLong(ctx, before_sleep_count);
    return ValkeyModule_ReplyWithLongLong(ctx, after_sleep_count);
}

int ValkeyModule_OnLoad(
    ValkeyModuleCtx *ctx,
    ValkeyModuleString **argv,
    int argc) {
    (void)argv;
    (void)argc;
    ValkeyModule_GetApi = (int (*)(const char *, void *))(*(void **)ctx);
    if (load_api("ValkeyModule_SetModuleAttribs", &ValkeyModule_SetModuleAttribs) != VALKEYMODULE_OK ||
        load_api("ValkeyModule_CreateCommand", &ValkeyModule_CreateCommand) != VALKEYMODULE_OK ||
        load_api("ValkeyModule_ReplyWithArray", &ValkeyModule_ReplyWithArray) != VALKEYMODULE_OK ||
        load_api("ValkeyModule_ReplyWithLongLong", &ValkeyModule_ReplyWithLongLong) != VALKEYMODULE_OK ||
        load_api("ValkeyModule_SubscribeToServerEvent", &ValkeyModule_SubscribeToServerEvent) != VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }
    ValkeyModule_SetModuleAttribs(ctx, "eventloophooks", 1, VALKEYMODULE_APIVER_1);
    if (ValkeyModule_CreateCommand(
            ctx, "eventloop.counts", event_loop_counts, "readonly fast", 0, 0, 0) !=
        VALKEYMODULE_OK) {
        return VALKEYMODULE_ERR;
    }
    ValkeyModuleEvent event = {VALKEYMODULE_EVENT_EVENTLOOP, 1};
    return ValkeyModule_SubscribeToServerEvent(ctx, event, event_loop_callback);
}
