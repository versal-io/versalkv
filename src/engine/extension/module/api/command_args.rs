use bytes::Bytes;
use libc::{c_char, c_int, size_t};
use std::ffi::CStr;

#[repr(C)]
#[derive(Clone, Copy)]
pub(crate) struct ValkeyModuleCommandArg {
    pub name: *const c_char,
    pub arg_type: c_int,
    pub key_spec_index: c_int,
    pub token: *const c_char,
    pub summary: *const c_char,
    pub since: *const c_char,
    pub flags: c_int,
    pub deprecated_since: *const c_char,
    pub subargs: *mut ValkeyModuleCommandArg,
    pub display_text: *const c_char,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ModuleCommandArg {
    pub name: Bytes,
    pub arg_type: &'static str,
    pub key_spec_index: Option<i32>,
    pub token: Option<Bytes>,
    pub summary: Option<Bytes>,
    pub since: Option<Bytes>,
    pub flags: Vec<&'static str>,
    pub deprecated_since: Option<Bytes>,
    pub arguments: Vec<ModuleCommandArg>,
    pub display_text: Option<Bytes>,
}

// SAFETY: args is null or a name-terminated v1 ABI tree with readable strings
// and element_size-byte elements in every array, including nested arrays.
pub(super) unsafe fn copy_command_args(
    args: *mut ValkeyModuleCommandArg,
    element_size: size_t,
) -> Option<Vec<ModuleCommandArg>> {
    if args.is_null() {
        return Some(Vec::new());
    }
    if element_size < std::mem::size_of::<ValkeyModuleCommandArg>()
        || element_size > isize::MAX as usize
    {
        return None;
    }
    let mut remaining = 4096usize;
    // SAFETY: the caller guarantees the tree's element and string allocations.
    unsafe { copy_arg_array(args, element_size, 0, &mut remaining) }
}

// SAFETY: args points to a readable name-terminated ABI array; its children
// obey the same stride and allocation contract.
unsafe fn copy_arg_array(
    args: *mut ValkeyModuleCommandArg,
    element_size: usize,
    depth: usize,
    remaining: &mut usize,
) -> Option<Vec<ModuleCommandArg>> {
    if depth >= 64 {
        return None;
    }
    let mut result = Vec::new();
    for index in 0..=4096usize {
        let offset = index.checked_mul(element_size)?;
        if offset > isize::MAX as usize {
            return None;
        }
        // SAFETY: the caller supplies a live array at this stride; unaligned
        // reads also support ABI versions with padding between elements.
        let arg = unsafe {
            args.cast::<u8>()
                .add(offset)
                .cast::<ValkeyModuleCommandArg>()
                .read_unaligned()
        };
        if arg.name.is_null() {
            return Some(result);
        }
        *remaining = remaining.checked_sub(1)?;
        let arg_type = match arg.arg_type {
            0 => "string",
            1 => "integer",
            2 => "double",
            3 => "key",
            4 => "pattern",
            5 => "unix-time",
            6 => "pure-token",
            7 => "oneof",
            8 => "block",
            _ => return None,
        };
        if arg.flags & !7 != 0 || (arg_type == "pure-token" && arg.token.is_null()) {
            return None;
        }
        let key_spec_index = if arg_type == "key" {
            if arg.key_spec_index < 0 {
                return None;
            }
            Some(arg.key_spec_index)
        } else {
            if !matches!(arg.key_spec_index, -1 | 0) {
                return None;
            }
            None
        };
        let is_container = matches!(arg_type, "oneof" | "block");
        if is_container == arg.subargs.is_null() {
            return None;
        }
        let arguments = if is_container {
            // SAFETY: the module's child array shares the validated ABI stride.
            unsafe { copy_arg_array(arg.subargs, element_size, depth + 1, remaining)? }
        } else {
            Vec::new()
        };
        // SAFETY: all non-null strings are readable until this deep copy returns.
        let copied = unsafe {
            ModuleCommandArg {
                name: copy_string(arg.name)?,
                arg_type,
                key_spec_index,
                token: copy_string(arg.token),
                summary: copy_string(arg.summary),
                since: copy_string(arg.since),
                flags: [(1, "optional"), (2, "multiple"), (4, "multiple_token")]
                    .into_iter()
                    .filter_map(|(bit, name)| (arg.flags & bit != 0).then_some(name))
                    .collect(),
                deprecated_since: copy_string(arg.deprecated_since),
                arguments,
                display_text: copy_string(arg.display_text),
            }
        };
        result.push(copied);
    }
    None
}

// SAFETY: value is null or a readable NUL-terminated string.
unsafe fn copy_string(value: *const c_char) -> Option<Bytes> {
    if value.is_null() {
        return None;
    }
    // SAFETY: the caller guarantees the string remains live during the copy.
    Some(Bytes::copy_from_slice(
        unsafe { CStr::from_ptr(value) }.to_bytes(),
    ))
}

#[cfg(test)]
pub(crate) mod test_support {
    use super::*;
    use std::ffi::CString;

    pub(crate) fn end_arg() -> ValkeyModuleCommandArg {
        ValkeyModuleCommandArg {
            name: std::ptr::null(),
            arg_type: 0,
            key_spec_index: -1,
            token: std::ptr::null(),
            summary: std::ptr::null(),
            since: std::ptr::null(),
            flags: 0,
            deprecated_since: std::ptr::null(),
            subargs: std::ptr::null_mut(),
            display_text: std::ptr::null(),
        }
    }

    pub(crate) fn copied_binary_tree() -> Vec<ModuleCommandArg> {
        let strings: Vec<_> = [
            &b"value\xff"[..],
            &b"\xff"[..],
            &b"T\xfe"[..],
            &b"S\xfd"[..],
            &b"1.\xfc"[..],
            &b"0.\xfb"[..],
            &b"fallback\xfa"[..],
            &b"group\xf9"[..],
            &b"choice\xf8"[..],
            &b"\xf7"[..],
            &b"child\xf6"[..],
            &b"\xf5"[..],
            &b"\xf4"[..],
            &b"\xf3"[..],
        ]
        .into_iter()
        .map(|bytes| CString::new(bytes).unwrap())
        .collect();
        let mut children = vec![
            ValkeyModuleCommandArg {
                name: strings[8].as_ptr(),
                arg_type: 6,
                token: strings[9].as_ptr(),
                summary: strings[10].as_ptr(),
                since: strings[11].as_ptr(),
                deprecated_since: strings[12].as_ptr(),
                display_text: strings[13].as_ptr(),
                ..end_arg()
            },
            end_arg(),
        ];
        let mut args = vec![
            ValkeyModuleCommandArg {
                name: strings[0].as_ptr(),
                token: strings[2].as_ptr(),
                summary: strings[3].as_ptr(),
                since: strings[4].as_ptr(),
                flags: 7,
                deprecated_since: strings[5].as_ptr(),
                display_text: strings[1].as_ptr(),
                ..end_arg()
            },
            ValkeyModuleCommandArg {
                name: strings[6].as_ptr(),
                ..end_arg()
            },
            ValkeyModuleCommandArg {
                name: strings[7].as_ptr(),
                arg_type: 8,
                subargs: children.as_mut_ptr(),
                ..end_arg()
            },
            end_arg(),
        ];
        // SAFETY: both arrays are name-terminated at the exact ABI stride, and
        // every CString stays live until the synchronous deep copy returns.
        let copied = unsafe {
            copy_command_args(
                args.as_mut_ptr(),
                std::mem::size_of::<ValkeyModuleCommandArg>(),
            )
        }
        .expect("valid binary argument tree");
        drop(args);
        drop(children);
        drop(strings);
        copied
    }
}

#[cfg(test)]
mod tests {
    use super::test_support::{copied_binary_tree, end_arg};
    use super::*;
    use std::ffi::CString;

    fn assert_bytes(value: impl AsRef<[u8]>, expected: &[u8]) {
        assert_eq!(value.as_ref(), expected);
    }

    #[test]
    fn command_metadata_args_preserve_binary_strings_after_inputs_are_freed() {
        let copied = copied_binary_tree();
        assert_eq!(copied.len(), 3);
        let leaf = &copied[0];
        assert_bytes(&leaf.name, b"value\xff");
        assert_bytes(leaf.display_text.as_ref().unwrap(), b"\xff");
        assert_bytes(leaf.token.as_ref().unwrap(), b"T\xfe");
        assert_bytes(leaf.summary.as_ref().unwrap(), b"S\xfd");
        assert_bytes(leaf.since.as_ref().unwrap(), b"1.\xfc");
        assert_bytes(leaf.deprecated_since.as_ref().unwrap(), b"0.\xfb");
        assert_eq!(leaf.arg_type, "string");
        assert_eq!(leaf.flags, ["optional", "multiple", "multiple_token"]);
        assert_eq!(leaf.key_spec_index, None);
        assert!(leaf.arguments.is_empty());

        let fallback = &copied[1];
        assert_bytes(&fallback.name, b"fallback\xfa");
        assert_eq!(fallback.display_text, None);
        assert_eq!(fallback.token, None);
        assert_eq!(fallback.summary, None);
        assert_eq!(fallback.since, None);
        assert_eq!(fallback.deprecated_since, None);

        let container = &copied[2];
        assert_bytes(&container.name, b"group\xf9");
        assert_eq!(container.arg_type, "block");
        assert_eq!(container.arguments.len(), 1);
        let child = &container.arguments[0];
        assert_bytes(&child.name, b"choice\xf8");
        assert_bytes(child.token.as_ref().unwrap(), b"\xf7");
        assert_bytes(child.summary.as_ref().unwrap(), b"child\xf6");
        assert_bytes(child.since.as_ref().unwrap(), b"\xf5");
        assert_bytes(child.deprecated_since.as_ref().unwrap(), b"\xf4");
        assert_bytes(child.display_text.as_ref().unwrap(), b"\xf3");
        assert_eq!(child.arg_type, "pure-token");
        assert!(child.flags.is_empty());
        assert!(child.arguments.is_empty());
    }

    #[test]
    fn command_metadata_args_accept_null_and_padded_ascii_inputs() {
        #[repr(C)]
        struct PaddedArg {
            arg: ValkeyModuleCommandArg,
            padding: [u64; 2],
        }

        // SAFETY: a null tree is supported even when no argument stride is given.
        assert_eq!(
            unsafe { copy_command_args(std::ptr::null_mut(), 0) },
            Some(Vec::new())
        );
        let name = CString::new("payload").unwrap();
        let mut args = vec![
            PaddedArg {
                arg: ValkeyModuleCommandArg {
                    name: name.as_ptr(),
                    ..end_arg()
                },
                padding: [u64::MAX; 2],
            },
            PaddedArg {
                arg: end_arg(),
                padding: [u64::MAX; 2],
            },
        ];
        // SAFETY: args starts with the ABI fields, uses the supplied padded
        // stride, and ends in a null name; name stays live throughout the copy.
        let copied = unsafe {
            copy_command_args(args.as_mut_ptr().cast(), std::mem::size_of::<PaddedArg>())
        }
        .unwrap();
        drop(args);
        drop(name);
        assert_eq!(copied.len(), 1);
        assert_bytes(&copied[0].name, b"payload");
        assert_eq!(copied[0].display_text, None);
        assert_eq!(copied[0].token, None);
        assert_eq!(copied[0].arg_type, "string");
    }
}
