// module_call_shim.c — C shim for variadic ValkeyModule C ABI functions.
//
// Rust stable does not support C variadic functions (extern "C" fn(...)).
// This shim provides the variadic wrappers that parse format strings and
// call into the Rust implementation via non-variadic entry points.
//
// Exported functions:
//   VM_Call                      — variadic format-string command invocation
//   VM_Replicate                 — variadic format-string command propagation
//   VM_CreateStringPrintf        — variadic format-string creation
//   VM_Log                       — variadic logging
//   VM_ReplyWithErrorFormat      — variadic formatted error reply
//   VM_StringToLongDouble        — long double parser
//   VM_ReplyWithLongDouble       — long double reply bridge
//   VM_HashSet                   — variadic hash field/value updates
//
// Internal entry points (implemented in Rust):
//   VM_CallWithReply  — execute command, return ValkeyModuleCallReply*
//   VM_CreateStringFromBuffer — create string from buffer
//   VM_CreateString   — create string (Rust)

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// ============================================================================
// Forward declarations of Rust entry points (non-variadic)
// ============================================================================

typedef struct ValkeyModuleCtx ValkeyModuleCtx;
typedef struct ValkeyModuleString ValkeyModuleString;
typedef struct ValkeyModuleCallReply ValkeyModuleCallReply;
typedef struct ValkeyModuleKey ValkeyModuleKey;

extern const char* VM_ModuleName(ValkeyModuleCtx* ctx);
extern void VM_LogBuffer(
    ValkeyModuleCtx* ctx,
    const char* level,
    const char* message,
    size_t message_len);

extern int VM_HashSetPairs(
    ValkeyModuleKey* key,
    int flags,
    void** fields,
    ValkeyModuleString** values,
    size_t pair_count);

// Format string flags (matching VALKEYMODULE_CALL_ARGV_* from valkeymodule.h):
//   !  -> REPLICATE       (1<<0)
//   A  -> NO_AOF          (1<<1)
//   R  -> NO_REPLICAS     (1<<2)
//   3  -> RESP_3          (1<<3)
//   0  -> RESP_AUTO       (1<<4)
//   C  -> RUN_AS_USER     (1<<5)
//   S  -> SCRIPT_MODE     (1<<6)
//   W  -> NO_WRITES       (1<<7)
//   E  -> ERRORS_AS_REPLIES (1<<8)
//   M  -> RESPECT_DENY_OOM (1<<9)
//   D  -> DRY_RUN         (1<<10, implies ERRORS_AS_REPLIES)
//   K  -> ALLOW_BLOCK     (1<<11)
//   X  -> REPLY_EXACT     (1<<12)
//
// Argument type specifiers (lowercase, consume va_args):
//   c  -> const char* (null-terminated C string)
//   s  -> ValkeyModuleString* (borrowed reference)
//   l  -> long long
//   b  -> const char* + size_t (buffer + length, two args)
//   v  -> ValkeyModuleString** + size_t (array + count)

#define FLAG_REPLICATE       (1 << 0)
#define FLAG_NO_AOF          (1 << 1)
#define FLAG_NO_REPLICAS     (1 << 2)
#define FLAG_RESP_3          (1 << 3)
#define FLAG_RESP_AUTO       (1 << 4)
#define FLAG_RUN_AS_USER     (1 << 5)
#define FLAG_SCRIPT_MODE     (1 << 6)
#define FLAG_NO_WRITES       (1 << 7)
#define FLAG_ERRORS_AS_REPL  (1 << 8)
#define FLAG_RESPECT_DENYOO  (1 << 9)
#define FLAG_DRY_RUN         (1 << 10)
#define FLAG_ALLOW_BLOCK     (1 << 11)
#define FLAG_REPLY_EXACT     (1 << 12)

// ============================================================================
// Rust entry points
// ============================================================================

extern ValkeyModuleCallReply* VM_CallWithReply(
    ValkeyModuleCtx* ctx,
    ValkeyModuleString** argv,
    int argc,
    int flags);

extern int VM_ReplicateCommand(
    ValkeyModuleCtx* ctx,
    ValkeyModuleString** argv,
    int argc);

extern ValkeyModuleString* VM_CreateStringFromBuffer(
    ValkeyModuleCtx* ctx,
    const char* data,
    size_t len);

extern ValkeyModuleString* VM_CreateString(
    ValkeyModuleCtx* ctx,
    const char* ptr,
    size_t len);

extern void VM_FreeCallReply(ValkeyModuleCallReply* reply);
extern int VM_ReplyWithErrorBuffer(
    ValkeyModuleCtx* ctx,
    const char* data,
    size_t len);
extern int VM_ReplyWithDoubleValue(ValkeyModuleCtx* ctx, double value);
extern const char* VM_StringPtrLenForShim(
    const ValkeyModuleString* str,
    size_t* len);
extern void VM_StringIncrRefForShim(ValkeyModuleString* str);
extern void VM_StringReleaseForShim(ValkeyModuleString* str);

static void release_argv(ValkeyModuleString** argv, int count) {
    for (int index = 0; index < count; index++) {
        VM_StringReleaseForShim(argv[index]);
    }
}

static int reserve_argv(
    ValkeyModuleString*** argv,
    ValkeyModuleString** stack,
    int* capacity,
    int count,
    size_t extra)
{
    if (extra > (size_t)(INT_MAX - count - 16)) return 0;
    int needed = count + (int)extra;
    if (needed <= *capacity) return 1;
    int new_capacity = needed + 16;
    if ((size_t)new_capacity > SIZE_MAX / sizeof(**argv)) return 0;
    ValkeyModuleString** grown = realloc(
        *argv == stack ? NULL : *argv, (size_t)new_capacity * sizeof(**argv));
    if (grown == NULL) return 0;
    if (*argv == stack) memcpy(grown, stack, (size_t)count * sizeof(**argv));
    *argv = grown;
    *capacity = new_capacity;
    return 1;
}

// ============================================================================
// VM_Call (variadic) — format-string based command invocation
// ============================================================================

ValkeyModuleCallReply* VM_Call(
    ValkeyModuleCtx* ctx,
    const char* cmdname,
    const char* fmt,
    ...)
{
    if (ctx == NULL || cmdname == NULL || fmt == NULL) {
        return NULL;
    }

    va_list ap;
    va_start(ap, fmt);

    // Compute flags before building argv; capacity grows for every argument slot.
    int flags = 0;
    const char* p = fmt;
    while (*p) {
        switch (*p) {
            // Argument type specifiers (lowercase) — consume va_args
            case 'c': case 's': case 'l': case 'b': case 'v': break;
            // Flag characters (uppercase / !) — set flags, no va_args consumed
            case '!': flags |= FLAG_REPLICATE; break;
            case 'A': flags |= FLAG_NO_AOF; break;
            case 'R': flags |= FLAG_NO_REPLICAS; break;
            case '3': flags |= FLAG_RESP_3; break;
            case '0': flags |= FLAG_RESP_AUTO; break;
            case 'C': flags |= FLAG_RUN_AS_USER; break;
            case 'S': flags |= FLAG_SCRIPT_MODE; break;
            case 'W': flags |= FLAG_NO_WRITES; break;
            case 'E': flags |= FLAG_ERRORS_AS_REPL; break;
            case 'M': flags |= FLAG_RESPECT_DENYOO; break;
            case 'D': flags |= (FLAG_DRY_RUN | FLAG_ERRORS_AS_REPL); break;
            case 'K': flags |= FLAG_ALLOW_BLOCK; break;
            case 'X': flags |= FLAG_REPLY_EXACT; break;
            default: break;
        }
        p++;
    }

    va_end(ap);
    va_start(ap, fmt);

    // Second pass: build argv array.
    ValkeyModuleString* argv_stack[128];
    ValkeyModuleString** argv = argv_stack;
    int argv_cap = 128;

    int idx = 0;
    argv[idx++] = VM_CreateString(NULL, cmdname, strlen(cmdname));
    p = fmt;
    while (*p) {
        if (!reserve_argv(&argv, argv_stack, &argv_cap, idx, 1)) goto call_build_failed;
        switch (*p) {
            case 'c': {
                const char* s = va_arg(ap, const char*);
                if (s != NULL) {
                    argv[idx] = VM_CreateString(NULL, s, strlen(s));
                } else {
                    argv[idx] = VM_CreateString(NULL, "", 0);
                }
                idx++;
                break;
            }
            case 's': {
                ValkeyModuleString* ms = va_arg(ap, ValkeyModuleString*);
                if (ms == NULL) goto call_build_failed;
                VM_StringIncrRefForShim(ms);
                argv[idx++] = ms;
                break;
            }
            case 'l': {
                long long val = va_arg(ap, long long);
                char buf[32];
                int n = snprintf(buf, sizeof(buf), "%lld", val);
                argv[idx++] = VM_CreateString(NULL, buf, (size_t)n);
                break;
            }
            case 'b': {
                const char* data = va_arg(ap, const char*);
                size_t len = va_arg(ap, size_t);
                argv[idx++] = VM_CreateString(NULL, data, len);
                break;
            }
            case 'v': {
                ValkeyModuleString** arr = va_arg(ap, ValkeyModuleString**);
                size_t count = va_arg(ap, size_t);
                if ((arr == NULL && count != 0) ||
                    !reserve_argv(&argv, argv_stack, &argv_cap, idx, count)) goto call_build_failed;
                for (size_t i = 0; i < count; i++) {
                    if (arr[i] == NULL) goto call_build_failed;
                    VM_StringIncrRefForShim(arr[i]);
                    argv[idx++] = arr[i];
                }
                break;
            }
            // Flags — no va_args consumed
            case '!': case 'A': case 'R': case '3': case '0':
            case 'C': case 'S': case 'W': case 'E': case 'M':
            case 'D': case 'K': case 'X':
                break;
            default:
                goto call_build_failed;
        }
        p++;
    }

    va_end(ap);

    // Call the Rust implementation
    ValkeyModuleCallReply* reply = VM_CallWithReply(ctx, argv, idx, flags);

    release_argv(argv, idx);
    if (argv != argv_stack) {
        free(argv);
    }

    return reply;

call_build_failed:
    va_end(ap);
    release_argv(argv, idx);
    if (argv != argv_stack) free(argv);
    errno = EINVAL;
    return NULL;
}

// ============================================================================
// VM_Replicate (variadic) — format-string based command propagation
// ============================================================================

int VM_Replicate(
    ValkeyModuleCtx* ctx,
    const char* cmdname,
    const char* fmt,
    ...)
{
    if (ctx == NULL || cmdname == NULL || fmt == NULL) {
        return 1;
    }

    va_list ap;
    va_start(ap, fmt);

    const char* p = fmt;

    ValkeyModuleString* argv_stack[128];
    ValkeyModuleString** argv = argv_stack;
    int argv_cap = 128;

    int idx = 0;
    argv[idx++] = VM_CreateString(NULL, cmdname, strlen(cmdname));
    p = fmt;
    while (*p) {
        if (!reserve_argv(&argv, argv_stack, &argv_cap, idx, 1)) goto replicate_build_failed;
        switch (*p) {
            case 'c': {
                const char* s = va_arg(ap, const char*);
                argv[idx++] = VM_CreateString(NULL, s != NULL ? s : "", s != NULL ? strlen(s) : 0);
                break;
            }
            case 's': {
                ValkeyModuleString* ms = va_arg(ap, ValkeyModuleString*);
                if (ms == NULL) goto replicate_build_failed;
                VM_StringIncrRefForShim(ms);
                argv[idx++] = ms;
                break;
            }
            case 'l': {
                long long val = va_arg(ap, long long);
                char buf[32];
                int n = snprintf(buf, sizeof(buf), "%lld", val);
                argv[idx++] = VM_CreateString(NULL, buf, (size_t)n);
                break;
            }
            case 'b': {
                const char* data = va_arg(ap, const char*);
                size_t len = va_arg(ap, size_t);
                argv[idx++] = VM_CreateString(NULL, data, len);
                break;
            }
            case 'v': {
                ValkeyModuleString** arr = va_arg(ap, ValkeyModuleString**);
                size_t count = va_arg(ap, size_t);
                if ((arr == NULL && count != 0) ||
                    !reserve_argv(&argv, argv_stack, &argv_cap, idx, count)) goto replicate_build_failed;
                for (size_t i = 0; i < count; i++) {
                    if (arr[i] == NULL) goto replicate_build_failed;
                    VM_StringIncrRefForShim(arr[i]);
                    argv[idx++] = arr[i];
                }
                break;
            }
            case '!': case 'A': case 'R': case '3': case '0':
            case 'C': case 'S': case 'W': case 'E': case 'M':
            case 'D': case 'K': case 'X':
                break;
            default:
                goto replicate_build_failed;
        }
        p++;
    }

    va_end(ap);

    int ret = VM_ReplicateCommand(ctx, argv, idx);
    release_argv(argv, idx);
    if (argv != argv_stack) {
        free(argv);
    }
    return ret;

replicate_build_failed:
    va_end(ap);
    release_argv(argv, idx);
    if (argv != argv_stack) free(argv);
    errno = EINVAL;
    return 1;
}

// ============================================================================
// VM_Log (variadic) — module logging
// ============================================================================

void VM_Log(
    ValkeyModuleCtx* ctx,
    const char* level,
    const char* fmt,
    ...)
{
    if (level == NULL) level = "notice";
    if (fmt == NULL) return;

    va_list ap;
    va_start(ap, fmt);
    va_list ap_copy;
    va_copy(ap_copy, ap);
    int needed = vsnprintf(NULL, 0, fmt, ap_copy);
    va_end(ap_copy);
    if (needed < 0) {
        va_end(ap);
        return;
    }

    char* message = (char*)malloc((size_t)needed + 1);
    if (message == NULL) {
        va_end(ap);
        return;
    }
    int written = vsnprintf(message, (size_t)needed + 1, fmt, ap);
    va_end(ap);
    if (written >= 0) {
        VM_LogBuffer(ctx, level, message, (size_t)written);
    }
    free(message);
}

// ============================================================================
// VM_CreateStringPrintf (variadic) — printf-style string creation
// ============================================================================

ValkeyModuleString* VM_CreateStringPrintf(
    ValkeyModuleCtx* ctx,
    const char* fmt,
    ...)
{
    if (ctx == NULL || fmt == NULL) {
        return NULL;
    }

    va_list ap;
    va_start(ap, fmt);

    va_list ap_copy;
    va_copy(ap_copy, ap);
    int needed = vsnprintf(NULL, 0, fmt, ap_copy);
    va_end(ap_copy);

    if (needed < 0) {
        va_end(ap);
        return NULL;
    }

    size_t buf_size = (size_t)needed + 1;
    char* buf = (char*)malloc(buf_size);
    if (buf == NULL) {
        va_end(ap);
        return NULL;
    }

    int written = vsnprintf(buf, buf_size, fmt, ap);
    va_end(ap);

    if (written < 0) {
        free(buf);
        return NULL;
    }

    ValkeyModuleString* result = VM_CreateStringFromBuffer(ctx, buf, (size_t)written);
    free(buf);
    return result;
}

ValkeyModuleString* VM_CreateStringFromLongDouble(
    ValkeyModuleCtx* ctx,
    long double value,
    int humanfriendly)
{
    char buf[512];
    int written = snprintf(
        buf,
        sizeof(buf),
        humanfriendly ? "%.17Lf" : "%.17Lg",
        value);
    if (written < 0 || (size_t)written >= sizeof(buf)) {
        return NULL;
    }
    if (humanfriendly) {
        char* dot = strchr(buf, '.');
        if (dot != NULL) {
            char* end = buf + written - 1;
            while (end > dot && *end == '0') {
                end--;
            }
            if (end == dot) {
                end--;
            }
            written = (int)(end - buf + 1);
            buf[written] = '\0';
        }
    }
    return VM_CreateStringFromBuffer(ctx, buf, (size_t)written);
}

int VM_ReplyWithErrorFormat(
    ValkeyModuleCtx* ctx,
    const char* fmt,
    ...)
{
    if (ctx == NULL || fmt == NULL) {
        return 1;
    }

    va_list ap;
    va_start(ap, fmt);
    va_list ap_copy;
    va_copy(ap_copy, ap);
    int needed = vsnprintf(NULL, 0, fmt, ap_copy);
    va_end(ap_copy);
    if (needed < 0) {
        va_end(ap);
        return 1;
    }

    char* buf = (char*)malloc((size_t)needed + 1);
    if (buf == NULL) {
        va_end(ap);
        return 1;
    }
    int written = vsnprintf(buf, (size_t)needed + 1, fmt, ap);
    va_end(ap);
    if (written < 0) {
        free(buf);
        return 1;
    }

    int result = VM_ReplyWithErrorBuffer(ctx, buf, (size_t)written);
    free(buf);
    return result;
}

int VM_StringToLongDouble(
    const ValkeyModuleString* str,
    long double* value)
{
    if (str == NULL || value == NULL) {
        return 1;
    }
    size_t len = 0;
    const char* data = VM_StringPtrLenForShim(str, &len);
    if (data == NULL) {
        return 1;
    }

    char* buf = (char*)malloc(len + 1);
    if (buf == NULL) {
        return 1;
    }
    memcpy(buf, data, len);
    buf[len] = '\0';

    char* end = NULL;
    long double parsed = strtold(buf, &end);
    int result = (end == buf + len) ? 0 : 1;
    if (result == 0) {
        *value = parsed;
    }
    free(buf);
    return result;
}

int VM_ReplyWithLongDouble(ValkeyModuleCtx* ctx, long double value)
{
    return VM_ReplyWithDoubleValue(ctx, (double)value);
}

extern int VM_HashGetField(ValkeyModuleKey *key, int flags, void *field, void *output);

int VM_HashGet(ValkeyModuleKey *key, int flags, ...)
{
    int result = VM_HashGetField(key, flags, NULL, NULL);
    if (result != 0) return result;
    va_list ap;
    va_start(ap, flags);
    void *field;
    while ((field = va_arg(ap, void *)) != NULL) {
        void *output = va_arg(ap, void *);
        result = VM_HashGetField(key, flags, field, output);
        if (result != 0) break;
    }
    va_end(ap);
    return result;
}

int VM_HashSet(ValkeyModuleKey* key, int flags, ...)
{
    va_list ap;
    va_start(ap, flags);
    va_list count_ap;
    va_copy(count_ap, ap);

    size_t pair_count = 0;
    while (va_arg(count_ap, void*) != NULL) {
        (void)va_arg(count_ap, ValkeyModuleString*);
        pair_count++;
    }
    va_end(count_ap);

    if (pair_count == 0) {
        va_end(ap);
        errno = ENOENT;
        return 0;
    }

    void** fields = (void**)malloc(pair_count * sizeof(*fields));
    ValkeyModuleString** values =
        (ValkeyModuleString**)malloc(pair_count * sizeof(*values));
    if (fields == NULL || values == NULL) {
        free(fields);
        free(values);
        va_end(ap);
        errno = ENOMEM;
        return 0;
    }

    for (size_t index = 0; index < pair_count; index++) {
        fields[index] = va_arg(ap, void*);
        values[index] = va_arg(ap, ValkeyModuleString*);
    }
    (void)va_arg(ap, void*);
    va_end(ap);

    int result = VM_HashSetPairs(key, flags, fields, values, pair_count);
    free(fields);
    free(values);
    if (result < 0) {
        errno = -result;
        return 0;
    }
    errno = result == 0 ? ENOENT : 0;
    return result;
}
