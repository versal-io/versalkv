use libc::{c_int, c_void};
use std::ffi::CStr;

use super::super::{VALKEYMODULE_ERR, VALKEYMODULE_OK};
use super::{
    acl, call, client_api, ctx, datatype_api, key_api, key_eviction_api, list_api, memory_api,
    pubsub_api, reply_api, scan, server_event_api, stream, string_obj_api, thread_safe_context_api,
    time_api, timer,
};

extern "C" {
    fn VM_Call();
    fn VM_Replicate();
    fn VM_CreateStringPrintf();
    fn VM_CreateStringFromLongDouble();
    fn VM_Log();
    fn VM_ReplyWithErrorFormat();
    fn VM_StringToLongDouble();
    fn VM_ReplyWithLongDouble();
    fn VM_HashSet();
}

// SAFETY: Called by C module code via VALKEYMODULE_GET_API macro; name and ptr are
// guaranteed non-null by the C caller, and ptr points to a valid function pointer
// variable.
pub(crate) unsafe extern "C" fn module_get_api_impl(
    name: *const libc::c_char,
    ptr: *mut *mut c_void,
) -> c_int {
    unsafe {
        if name.is_null() || ptr.is_null() {
            return VALKEYMODULE_ERR;
        }
        let api_name = match CStr::from_ptr(name).to_str() {
            Ok(s) => s,
            Err(_) => return VALKEYMODULE_ERR,
        };

        let Some(func_ptr) = resolve_module_api(api_name) else {
            log::warn!("Module requested unimplemented API: {}", api_name);
            return VALKEYMODULE_ERR;
        };

        *ptr = func_ptr;
        VALKEYMODULE_OK
    }
}

struct ModuleApiEntry {
    name: &'static str,
    address: fn() -> *mut c_void,
}

macro_rules! module_api_entries {
    ($($name:literal => $address:expr),* $(,)?) => {
        static MODULE_API_ENTRIES: &[ModuleApiEntry] = &[
            $(ModuleApiEntry {
                name: $name,
                address: || $address,
            },)*
        ];
    };
}

module_api_entries! {
        "ValkeyModule_Alloc" => memory_api::ValkeyModule_Alloc as *mut c_void,
        "ValkeyModule_Calloc" => memory_api::ValkeyModule_Calloc as *mut c_void,
        "ValkeyModule_Realloc" => memory_api::ValkeyModule_Realloc as *mut c_void,
        "ValkeyModule_Free" => memory_api::ValkeyModule_Free as *mut c_void,
        "ValkeyModule_Strdup" => memory_api::ValkeyModule_Strdup as *mut c_void,
        "ValkeyModule_TryAlloc" => memory_api::ValkeyModule_Alloc as *mut c_void,
        "ValkeyModule_TryCalloc" => memory_api::ValkeyModule_Calloc as *mut c_void,
        "ValkeyModule_TryRealloc" => memory_api::ValkeyModule_Realloc as *mut c_void,
        "ValkeyModule_FreeMemoryIfNeeded" => {
            memory_api::ValkeyModule_FreeMemoryIfNeeded as *mut c_void
        },
        "ValkeyModule_ClusterKeySlotC" => {
            super::cluster_slot_api::ValkeyModule_ClusterKeySlotC as *mut c_void
        },
        "ValkeyModule_ClusterKeySlot" => {
            super::cluster_slot_api::ValkeyModule_ClusterKeySlot as *mut c_void
        },
        "ValkeyModule_ClusterCanonicalKeyNameInSlot" => {
            super::cluster_slot_api::ValkeyModule_ClusterCanonicalKeyNameInSlot as *mut c_void
        },
        "ValkeyModule_CreateCommand" => ctx::ValkeyModule_CreateCommand as *mut c_void,
        "ValkeyModule_GetCommand" => ctx::ValkeyModule_GetCommand as *mut c_void,
        "ValkeyModule_CreateSubcommand" => ctx::ValkeyModule_CreateSubcommand as *mut c_void,
        "ValkeyModule_SetCommandACLCategories" => {
            acl::ValkeyModule_SetCommandACLCategories as *mut c_void
        },
        "ValkeyModule_AddACLCategory" => acl::ValkeyModule_AddACLCategory as *mut c_void,
        "ValkeyModule_SetCommandInfo" => {
            super::command_info_api::ValkeyModule_SetCommandInfo as *mut c_void
        },
        "ValkeyModule_SetModuleAttribs" => ctx::ValkeyModule_SetModuleAttribs as *mut c_void,
        "ValkeyModule_IsModuleNameBusy" => ctx::ValkeyModule_IsModuleNameBusy as *mut c_void,
        "ValkeyModule_ReplyWithLongLong" => {
            reply_api::ValkeyModule_ReplyWithLongLong as *mut c_void
        },
        "ValkeyModule_ReplyWithError" => reply_api::ValkeyModule_ReplyWithError as *mut c_void,
        "ValkeyModule_ReplyWithErrorFormat" => VM_ReplyWithErrorFormat as *mut c_void,
        "ValkeyModule_ReplyWithSimpleString" => {
            reply_api::ValkeyModule_ReplyWithSimpleString as *mut c_void
        },
        "ValkeyModule_ReplyWithArray" => reply_api::ValkeyModule_ReplyWithArray as *mut c_void,
        "ValkeyModule_ReplyWithMap" => reply_api::ValkeyModule_ReplyWithMap as *mut c_void,
        "ValkeyModule_ReplyWithSet" => reply_api::ValkeyModule_ReplyWithSet as *mut c_void,
        "ValkeyModule_ReplyWithAttribute" => {
            reply_api::ValkeyModule_ReplyWithAttribute as *mut c_void
        },
        "ValkeyModule_ReplyWithNullArray" => {
            reply_api::ValkeyModule_ReplyWithNullArray as *mut c_void
        },
        "ValkeyModule_ReplyWithEmptyArray" => {
            reply_api::ValkeyModule_ReplyWithEmptyArray as *mut c_void
        },
        "ValkeyModule_ReplySetArrayLength" => {
            reply_api::ValkeyModule_ReplySetArrayLength as *mut c_void
        },
        "ValkeyModule_ReplyWithString" => reply_api::ValkeyModule_ReplyWithString as *mut c_void,
        "ValkeyModule_ReplyWithEmptyString" => {
            reply_api::ValkeyModule_ReplyWithEmptyString as *mut c_void
        },
        "ValkeyModule_ReplyWithNull" => reply_api::ValkeyModule_ReplyWithNull as *mut c_void,
        "ValkeyModule_ReplyWithBool" => reply_api::ValkeyModule_ReplyWithBool as *mut c_void,
        "ValkeyModule_ReplyWithDouble" => reply_api::ValkeyModule_ReplyWithDouble as *mut c_void,
        "ValkeyModule_ReplyWithLongDouble" => VM_ReplyWithLongDouble as *mut c_void,
        "ValkeyModule_ReplyWithBigNumber" => {
            reply_api::ValkeyModule_ReplyWithBigNumber as *mut c_void
        },
        "ValkeyModule_ReplyWithStringBuffer" => {
            reply_api::ValkeyModule_ReplyWithStringBuffer as *mut c_void
        },
        "ValkeyModule_ReplyWithCString" => reply_api::ValkeyModule_ReplyWithCString as *mut c_void,
        "ValkeyModule_ReplyWithVerbatimString" => {
            reply_api::ValkeyModule_ReplyWithVerbatimString as *mut c_void
        },
        "ValkeyModule_ReplyWithCallReply" => call::ValkeyModule_ReplyWithCallReply as *mut c_void,
        "ValkeyModule_GetOpenKeyModesAll" => {
            key_api::ValkeyModule_GetOpenKeyModesAll as *mut c_void
        },
        "ValkeyModule_OpenKey" => key_api::ValkeyModule_OpenKey as *mut c_void,
        "ValkeyModule_CloseKey" => key_api::ValkeyModule_CloseKey as *mut c_void,
        "ValkeyModule_KeyType" => key_api::ValkeyModule_KeyType as *mut c_void,
        "ValkeyModule_ValueLength" => key_api::ValkeyModule_ValueLength as *mut c_void,
        "ValkeyModule_KeyExists" => key_api::ValkeyModule_KeyExists as *mut c_void,
        "ValkeyModule_DeleteKey" => key_api::ValkeyModule_DeleteKey as *mut c_void,
        "ValkeyModule_UnlinkKey" => key_api::ValkeyModule_UnlinkKey as *mut c_void,
        "ValkeyModule_SetLRU" => key_eviction_api::ValkeyModule_SetLRU as *mut c_void,
        "ValkeyModule_GetLRU" => key_eviction_api::ValkeyModule_GetLRU as *mut c_void,
        "ValkeyModule_SetLFU" => key_eviction_api::ValkeyModule_SetLFU as *mut c_void,
        "ValkeyModule_GetLFU" => key_eviction_api::ValkeyModule_GetLFU as *mut c_void,
        "ValkeyModule_HashSet" => VM_HashSet as *mut c_void,
        "ValkeyModule_HashSetStringRef" => {
            super::hash_api::ValkeyModule_HashSetStringRef as *mut c_void
        },
        "ValkeyModule_HashHasStringRef" => {
            super::hash_api::ValkeyModule_HashHasStringRef as *mut c_void
        },
        "ValkeyModule_ListGet" => list_api::ValkeyModule_ListGet as *mut c_void,
        "ValkeyModule_ListSet" => list_api::ValkeyModule_ListSet as *mut c_void,
        "ValkeyModule_ListInsert" => list_api::ValkeyModule_ListInsert as *mut c_void,
        "ValkeyModule_ListDelete" => list_api::ValkeyModule_ListDelete as *mut c_void,
        "ValkeyModule_PublishMessage" => pubsub_api::ValkeyModule_PublishMessage as *mut c_void,
        "ValkeyModule_PublishMessageShard" => {
            pubsub_api::ValkeyModule_PublishMessageShard as *mut c_void
        },
        "ValkeyModule_CreateString" => string_obj_api::ValkeyModule_CreateString as *mut c_void,
        "ValkeyModule_CreateStringFromLongLong" => {
            string_obj_api::ValkeyModule_CreateStringFromLongLong as *mut c_void
        },
        "ValkeyModule_CreateStringFromULongLong" => {
            string_obj_api::ValkeyModule_CreateStringFromULongLong as *mut c_void
        },
        "ValkeyModule_CreateStringFromString" => {
            string_obj_api::ValkeyModule_CreateStringFromString as *mut c_void
        },
        "ValkeyModule_FreeString" => string_obj_api::ValkeyModule_FreeString as *mut c_void,
        "ValkeyModule_StringPtrLen" => string_obj_api::ValkeyModule_StringPtrLen as *mut c_void,
        "ValkeyModule_StringToLongLong" => {
            string_obj_api::ValkeyModule_StringToLongLong as *mut c_void
        },
        "ValkeyModule_StringToULongLong" => {
            string_obj_api::ValkeyModule_StringToULongLong as *mut c_void
        },
        "ValkeyModule_StringToDouble" => string_obj_api::ValkeyModule_StringToDouble as *mut c_void,
        "ValkeyModule_StringToLongDouble" => VM_StringToLongDouble as *mut c_void,
        "ValkeyModule_StringAppendBuffer" => call::ValkeyModule_StringAppendBuffer as *mut c_void,
        "ValkeyModule_RetainString" => call::ValkeyModule_RetainString as *mut c_void,
        "ValkeyModule_StringSet" => call::ValkeyModule_StringSet as *mut c_void,
        "ValkeyModule_StringDMA" => call::ValkeyModule_StringDMA as *mut c_void,
        "ValkeyModule_StringTruncate" => call::ValkeyModule_StringTruncate as *mut c_void,
        "ValkeyModule_StringCompare" => call::ValkeyModule_StringCompare as *mut c_void,
        "ValkeyModule_CreateStringPrintf" => VM_CreateStringPrintf as *mut c_void,
        "ValkeyModule_CreateStringFromLongDouble" => VM_CreateStringFromLongDouble as *mut c_void,
        "ValkeyModule_MallocSizeString" => call::ValkeyModule_MallocSizeString as *mut c_void,
        "ValkeyModule_TrimStringAllocation" => {
            call::ValkeyModule_TrimStringAllocation as *mut c_void
        },
        "ValkeyModule_AutoMemory" => string_obj_api::ValkeyModule_AutoMemory as *mut c_void,
        "ValkeyModule_CreateDataType" => datatype_api::ValkeyModule_CreateDataType as *mut c_void,
        "ValkeyModule_ModuleTypeSetValue" => {
            datatype_api::ValkeyModule_ModuleTypeSetValue as *mut c_void
        },
        "ValkeyModule_ModuleTypeGetValue" => {
            datatype_api::ValkeyModule_ModuleTypeGetValue as *mut c_void
        },
        "ValkeyModule_ModuleTypeReplaceValue" => {
            datatype_api::ValkeyModule_ModuleTypeReplaceValue as *mut c_void
        },
        "ValkeyModule_ModuleTypeGetType" => {
            datatype_api::ValkeyModule_ModuleTypeGetType as *mut c_void
        },
        "ValkeyModule_CallReplyType" => call::ValkeyModule_CallReplyType as *mut c_void,
        "ValkeyModule_CallReplyLength" => call::ValkeyModule_CallReplyLength as *mut c_void,
        "ValkeyModule_CallReplyArrayElement" => {
            call::ValkeyModule_CallReplyArrayElement as *mut c_void
        },
        "ValkeyModule_CallReplyInteger" => call::ValkeyModule_CallReplyInteger as *mut c_void,
        "ValkeyModule_CallReplyDouble" => call::ValkeyModule_CallReplyDouble as *mut c_void,
        "ValkeyModule_CallReplyBool" => call::ValkeyModule_CallReplyBool as *mut c_void,
        "ValkeyModule_CallReplyStringPtr" => call::ValkeyModule_CallReplyStringPtr as *mut c_void,
        "ValkeyModule_CallReplyProto" => call::ValkeyModule_CallReplyProto as *mut c_void,
        "ValkeyModule_FreeCallReply" => call::ValkeyModule_FreeCallReply as *mut c_void,
        "ValkeyModule_CreateStringFromCallReply" => {
            call::ValkeyModule_CreateStringFromCallReply as *mut c_void
        },
        "ValkeyModule_CallReplyBigNumber" => call::ValkeyModule_CallReplyBigNumber as *mut c_void,
        "ValkeyModule_CallReplyVerbatim" => call::ValkeyModule_CallReplyVerbatim as *mut c_void,
        "ValkeyModule_CallReplySetElement" => call::ValkeyModule_CallReplySetElement as *mut c_void,
        "ValkeyModule_CallReplyMapElement" => call::ValkeyModule_CallReplyMapElement as *mut c_void,
        "ValkeyModule_CallReplyAttribute" => call::ValkeyModule_CallReplyAttribute as *mut c_void,
        "ValkeyModule_CallReplyAttributeElement" => {
            call::ValkeyModule_CallReplyAttributeElement as *mut c_void
        },
        "ValkeyModule_GetContextFlags" => call::ValkeyModule_GetContextFlags as *mut c_void,
        "ValkeyModule_SetContextProtectedMode" => {
            call::ValkeyModule_SetContextProtectedMode as *mut c_void
        },
        "ValkeyModule_NotifyKeyspaceEvent" => call::ValkeyModule_NotifyKeyspaceEvent as *mut c_void,
        "ValkeyModule_Log" => VM_Log as *mut c_void,
        "ValkeyModule_WrongArity" => call::ValkeyModule_WrongArity as *mut c_void,
        "ValkeyModule_GetSelectedDb" => call::ValkeyModule_GetSelectedDb as *mut c_void,
        "ValkeyModule_SelectDb" => call::ValkeyModule_SelectDb as *mut c_void,
        "ValkeyModule_GetClientId" => client_api::ValkeyModule_GetClientId as *mut c_void,
        "ValkeyModule_GetClientInfoById" => {
            client_api::ValkeyModule_GetClientInfoById as *mut c_void
        },
        "ValkeyModule_GetClientNameById" => {
            client_api::ValkeyModule_GetClientNameById as *mut c_void
        },
        "ValkeyModule_SetClientNameById" => {
            client_api::ValkeyModule_SetClientNameById as *mut c_void
        },
        "ValkeyModule_GetClientCertificate" => {
            client_api::ValkeyModule_GetClientCertificate as *mut c_void
        },
        "ValkeyModule_GetDetachedThreadSafeContext" => {
            thread_safe_context_api::ValkeyModule_GetDetachedThreadSafeContext as *mut c_void
        },
        "ValkeyModule_FreeThreadSafeContext" => {
            thread_safe_context_api::ValkeyModule_FreeThreadSafeContext as *mut c_void
        },
        "ValkeyModule_Milliseconds" => time_api::ValkeyModule_Milliseconds as *mut c_void,
        "ValkeyModule_MonotonicMicroseconds" => {
            time_api::ValkeyModule_MonotonicMicroseconds as *mut c_void
        },
        "ValkeyModule_Microseconds" => time_api::ValkeyModule_Microseconds as *mut c_void,
        "ValkeyModule_CachedMicroseconds" => {
            time_api::ValkeyModule_CachedMicroseconds as *mut c_void
        },
        "ValkeyModule_ResetDataset" => call::ValkeyModule_ResetDataset as *mut c_void,
        "ValkeyModule_DbSize" => call::ValkeyModule_DbSize as *mut c_void,
        "ValkeyModule_RandomKey" => call::ValkeyModule_RandomKey as *mut c_void,
        "ValkeyModule_Assert" => call::ValkeyModule_Assert as *mut c_void,
        "ValkeyModule_Call" => VM_Call as *mut c_void,
        "ValkeyModule_Replicate" => VM_Replicate as *mut c_void,
        "ValkeyModule_ReplicateVerbatim" => call::ValkeyModule_ReplicateVerbatim as *mut c_void,
        "ValkeyModule_LatencyAddSample" => call::ValkeyModule_LatencyAddSample as *mut c_void,
        "ValkeyModule_GetCurrentCommandName" => {
            call::ValkeyModule_GetCurrentCommandName as *mut c_void
        },
        "ValkeyModule_RedactClientCommandArgument" => {
            call::ValkeyModule_RedactClientCommandArgument as *mut c_void
        },
        "ValkeyModule_CreateModuleUser" => acl::ValkeyModule_CreateModuleUser as *mut c_void,
        "ValkeyModule_FreeModuleUser" => acl::ValkeyModule_FreeModuleUser as *mut c_void,
        "ValkeyModule_SetContextUser" => acl::ValkeyModule_SetContextUser as *mut c_void,
        "ValkeyModule_SetModuleUserACL" => acl::ValkeyModule_SetModuleUserACL as *mut c_void,
        "ValkeyModule_GetCurrentUserName" => acl::ValkeyModule_GetCurrentUserName as *mut c_void,
        "ValkeyModule_GetModuleUserFromUserName" => {
            acl::ValkeyModule_GetModuleUserFromUserName as *mut c_void
        },
        "ValkeyModule_ACLCheckCommandPermissions" => {
            acl::ValkeyModule_ACLCheckCommandPermissions as *mut c_void
        },
        "ValkeyModule_ACLCheckKeyPermissions" => {
            acl::ValkeyModule_ACLCheckKeyPermissions as *mut c_void
        },
        "ValkeyModule_ACLCheckChannelPermissions" => {
            acl::ValkeyModule_ACLCheckChannelPermissions as *mut c_void
        },
        "ValkeyModule_ACLCheckPermissions" => acl::ValkeyModule_ACLCheckPermissions as *mut c_void,
        "ValkeyModule_ACLCheckKeyPrefixPermissions" => {
            acl::ValkeyModule_ACLCheckKeyPrefixPermissions as *mut c_void
        },
        "ValkeyModule_ACLAddLogEntry" => acl::ValkeyModule_ACLAddLogEntry as *mut c_void,
        "ValkeyModule_AuthenticateClientWithUser" => {
            acl::ValkeyModule_AuthenticateClientWithUser as *mut c_void
        },
        "ValkeyModule_AuthenticateClientWithACLUser" => {
            acl::ValkeyModule_AuthenticateClientWithACLUser as *mut c_void
        },
        "ValkeyModule_CreateTimer" => timer::ValkeyModule_CreateTimer as *mut c_void,
        "ValkeyModule_StopTimer" => timer::ValkeyModule_StopTimer as *mut c_void,
        "ValkeyModule_GetTimerInfo" => timer::ValkeyModule_GetTimerInfo as *mut c_void,
        "ValkeyModule_ScanCursorCreate" => scan::ValkeyModule_ScanCursorCreate as *mut c_void,
        "ValkeyModule_ScanCursorRestart" => scan::ValkeyModule_ScanCursorRestart as *mut c_void,
        "ValkeyModule_ScanCursorDestroy" => scan::ValkeyModule_ScanCursorDestroy as *mut c_void,
        "ValkeyModule_Scan" => scan::ValkeyModule_Scan as *mut c_void,
        "ValkeyModule_ScanKey" => scan::ValkeyModule_ScanKey as *mut c_void,
        "ValkeyModule_CreateStringFromStreamID" => {
            stream::ValkeyModule_CreateStringFromStreamID as *mut c_void
        },
        "ValkeyModule_StringToStreamID" => stream::ValkeyModule_StringToStreamID as *mut c_void,
        "ValkeyModule_StreamAdd" => stream::ValkeyModule_StreamAdd as *mut c_void,
        "ValkeyModule_StreamDelete" => stream::ValkeyModule_StreamDelete as *mut c_void,
        "ValkeyModule_StreamIteratorStart" => {
            stream::ValkeyModule_StreamIteratorStart as *mut c_void
        },
        "ValkeyModule_StreamIteratorStop" => stream::ValkeyModule_StreamIteratorStop as *mut c_void,
        "ValkeyModule_StreamIteratorNextID" => {
            stream::ValkeyModule_StreamIteratorNextID as *mut c_void
        },
        "ValkeyModule_StreamIteratorNextField" => {
            stream::ValkeyModule_StreamIteratorNextField as *mut c_void
        },
        "ValkeyModule_StreamIteratorDelete" => {
            stream::ValkeyModule_StreamIteratorDelete as *mut c_void
        },
        "ValkeyModule_StreamTrimByLength" => stream::ValkeyModule_StreamTrimByLength as *mut c_void,
        "ValkeyModule_StreamTrimByID" => stream::ValkeyModule_StreamTrimByID as *mut c_void,
        "ValkeyModule_GetServerVersion" => call::ValkeyModule_GetServerVersion as *mut c_void,
        "ValkeyModule_RegisterInfoFunc" => call::ValkeyModule_RegisterInfoFunc as *mut c_void,
        "ValkeyModule_GetServerInfo" => call::ValkeyModule_GetServerInfo as *mut c_void,
        "ValkeyModule_FreeServerInfo" => call::ValkeyModule_FreeServerInfo as *mut c_void,
        "ValkeyModule_ServerInfoGetFieldC" => call::ValkeyModule_ServerInfoGetFieldC as *mut c_void,
        "ValkeyModule_SubscribeToKeyspaceEvents" => {
            call::ValkeyModule_SubscribeToKeyspaceEvents as *mut c_void
        },
        "ValkeyModule_SubscribeToServerEvent" => {
            server_event_api::ValkeyModule_SubscribeToServerEvent as *mut c_void
        },
        "ValkeyModule_IsSubEventSupported" => {
            server_event_api::ValkeyModule_IsSubEventSupported as *mut c_void
        },
}

fn resolve_module_api(api_name: &str) -> Option<*mut c_void> {
    MODULE_API_ENTRIES
        .iter()
        .find(|entry| entry.name == api_name)
        .map(|entry| (entry.address)())
}

#[cfg(test)]
mod tests {
    use super::thread_safe_context_api::VM_ModuleName;
    use super::{resolve_module_api, MODULE_API_ENTRIES};
    use crate::engine::extension::module::{
        create_module_cmd_ctx, create_module_ctx, free_module_ctx, ValkeyModuleCtx,
        ValkeyModuleString,
    };
    use libc::c_void;
    use std::collections::HashSet;
    use std::ffi::CStr;

    #[test]
    fn declarative_api_names_are_unique_and_resolve_to_their_addresses() {
        let mut names = HashSet::new();

        for entry in MODULE_API_ENTRIES {
            assert!(names.insert(entry.name), "duplicate API: {}", entry.name);
            let expected = (entry.address)();
            assert!(!expected.is_null(), "null API address: {}", entry.name);
            assert_eq!(resolve_module_api(entry.name), Some(expected));
        }
        assert_eq!(names.len(), MODULE_API_ENTRIES.len());
        assert!(resolve_module_api("ValkeyModule_NotImplemented").is_none());
    }

    #[test]
    fn resolves_misc_module_conversion_apis() {
        assert!(resolve_module_api("ValkeyModule_GetServerVersion").is_some());
        assert!(resolve_module_api("ValkeyModule_CreateStringFromLongDouble").is_some());
        assert!(resolve_module_api("ValkeyModule_CreateStringFromULongLong").is_some());
        assert!(resolve_module_api("ValkeyModule_StringToULongLong").is_some());
        assert!(resolve_module_api("ValkeyModule_ResetDataset").is_some());
        assert!(resolve_module_api("ValkeyModule_DbSize").is_some());
        assert!(resolve_module_api("ValkeyModule_RandomKey").is_some());
        assert!(resolve_module_api("ValkeyModule_SetLRU").is_some());
        assert!(resolve_module_api("ValkeyModule_GetLRU").is_some());
        assert!(resolve_module_api("ValkeyModule_SetLFU").is_some());
        assert!(resolve_module_api("ValkeyModule_GetLFU").is_some());
        assert!(resolve_module_api("ValkeyModule_GetClientId").is_some());
        assert!(resolve_module_api("ValkeyModule_GetClientInfoById").is_some());
        assert!(resolve_module_api("ValkeyModule_GetClientNameById").is_some());
        assert!(resolve_module_api("ValkeyModule_SetClientNameById").is_some());
        assert!(resolve_module_api("ValkeyModule_GetClientCertificate").is_some());
    }

    #[test]
    fn resolves_event_loop_server_event_apis() {
        assert!(resolve_module_api("ValkeyModule_SubscribeToServerEvent").is_some());
        assert!(resolve_module_api("ValkeyModule_IsSubEventSupported").is_some());
    }

    #[test]
    fn detached_thread_safe_context_preserves_module_identity_and_frees_independently() {
        // SAFETY: These function types model module ABI entries that accept raw
        // contexts; callers must pass contexts allocated by this module runtime.
        type GetDetached = unsafe extern "C" fn(*mut ValkeyModuleCtx) -> *mut ValkeyModuleCtx;
        type FreeDetached = unsafe extern "C" fn(*mut ValkeyModuleCtx);

        let module_ref = 0x1234usize as *mut c_void;
        let internal =
            create_module_cmd_ctx("misc", "test.log_tsctx", module_ref, std::ptr::null_mut());
        let original = Box::into_raw(create_module_ctx(Box::into_raw(internal)));
        let get_detached = resolve_module_api("ValkeyModule_GetDetachedThreadSafeContext")
            .expect("detached context API must resolve");
        let free_detached = resolve_module_api("ValkeyModule_FreeThreadSafeContext")
            .expect("detached context free API must resolve");

        // SAFETY: The resolver returns functions with the declared module ABI,
        // and original is a live context allocated by create_module_ctx.
        let detached = unsafe {
            let get_detached: GetDetached = std::mem::transmute(get_detached);
            get_detached(original)
        };
        assert!(!detached.is_null());
        // SAFETY: detached and original are live contexts returned by the module API.
        unsafe {
            assert_ne!((*detached).internal, (*original).internal);
            assert_eq!((*(*detached).internal).module_ref, module_ref);
            assert!((*(*detached).internal).runtime_handle.is_null());
            assert_eq!((*(*original).internal).module_ref, module_ref);
            assert_eq!(CStr::from_ptr(VM_ModuleName(detached)).to_bytes(), b"misc");
            assert!(resolve_module_api("ValkeyModule_ThreadSafeContextLock").is_none());
            assert!(resolve_module_api("ValkeyModule_ThreadSafeContextUnlock").is_none());

            let free_detached: FreeDetached = std::mem::transmute(free_detached);
            free_detached(detached);
            assert_eq!((*(*original).internal).module_ref, module_ref);
            free_module_ctx(original);
        }
    }

    #[test]
    fn monotonic_microseconds_api_resolves_and_never_moves_backwards() {
        type MonotonicMicroseconds = extern "C" fn() -> u64;

        let monotonic = resolve_module_api("ValkeyModule_MonotonicMicroseconds")
            .expect("monotonic microseconds API must resolve");
        // SAFETY: The resolver returns the declared no-argument module ABI.
        let monotonic: MonotonicMicroseconds = unsafe { std::mem::transmute(monotonic) };
        let first = monotonic();
        let second = monotonic();

        assert!(second >= first);
    }

    #[test]
    fn open_key_modes_api_resolves_and_reports_every_supported_mode() {
        type GetOpenKeyModesAll = extern "C" fn() -> libc::c_int;

        let get_modes = resolve_module_api("ValkeyModule_GetOpenKeyModesAll")
            .expect("open key modes API must resolve");
        // SAFETY: The resolver returns the declared no-argument module ABI.
        let get_modes: GetOpenKeyModesAll = unsafe { std::mem::transmute(get_modes) };

        assert_eq!(get_modes(), 0x1f0003);
    }

    #[test]
    fn cluster_slot_apis_resolve_and_round_trip_every_slot() {
        // SAFETY: These function types model module ABI entries whose pointer
        // arguments must remain live and readable for each synchronous call.
        type ClusterKeySlotC = unsafe extern "C" fn(*const libc::c_char, usize) -> u32;
        type ClusterKeySlot = unsafe extern "C" fn(*mut ValkeyModuleString) -> u32;
        type ClusterCanonicalKeyNameInSlot = extern "C" fn(u32) -> *const libc::c_char;

        let key_slot_c = resolve_module_api("ValkeyModule_ClusterKeySlotC")
            .expect("cluster key slot C API must resolve");
        let key_slot = resolve_module_api("ValkeyModule_ClusterKeySlot")
            .expect("cluster key slot string API must resolve");
        let canonical = resolve_module_api("ValkeyModule_ClusterCanonicalKeyNameInSlot")
            .expect("cluster canonical key API must resolve");

        // SAFETY: The resolver returns functions with the declared module ABI.
        let key_slot_c: ClusterKeySlotC = unsafe { std::mem::transmute(key_slot_c) };
        // SAFETY: The resolver returns functions with the declared module ABI.
        let key_slot: ClusterKeySlot = unsafe { std::mem::transmute(key_slot) };
        // SAFETY: The resolver returns functions with the declared module ABI.
        let canonical: ClusterCanonicalKeyNameInSlot = unsafe { std::mem::transmute(canonical) };

        for slot in 0..16_384 {
            let tag = canonical(slot);
            assert!(!tag.is_null());
            // SAFETY: canonical returns a stable NUL-terminated string for valid slots.
            let tag = unsafe { CStr::from_ptr(tag) }.to_bytes();
            let mut key_bytes = Vec::with_capacity(tag.len() + 4);
            key_bytes.extend_from_slice(b"x{");
            key_bytes.extend_from_slice(tag);
            key_bytes.extend_from_slice(b"}y");
            let mut key = Box::new(ValkeyModuleString::from_bytes(&key_bytes));

            // SAFETY: key and key_bytes remain live for both synchronous calls.
            unsafe {
                assert_eq!(key_slot(&mut **key), slot);
                assert_eq!(key_slot_c(key_bytes.as_ptr().cast(), key_bytes.len()), slot);
            }
        }

        assert!(canonical(16_384).is_null());
    }
}
