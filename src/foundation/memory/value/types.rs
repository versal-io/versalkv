// SPDX-License-Identifier: BSD-3-Clause

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Type {
    String,
    List,
    Hash,
    Set,
    ZSet,
    Stream,
}

impl Type {
    pub fn name(self) -> &'static str {
        match self {
            Type::String => "string",
            Type::List => "list",
            Type::Hash => "hash",
            Type::Set => "set",
            Type::ZSet => "zset",
            Type::Stream => "stream",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    Int,
    Embstr,
    Raw,
    Listpack,
    Quicklist,
    Hashtable,
    Intset,
    Skiplist,
    Btree,
    StreamP,
}

impl Encoding {
    pub fn name(self) -> &'static str {
        match self {
            Encoding::Int => "int",
            Encoding::Embstr => "embstr",
            Encoding::Raw => "raw",
            Encoding::Listpack => "listpack",
            Encoding::Quicklist => "quicklist",
            Encoding::Hashtable => "hashtable",
            Encoding::Intset => "intset",
            Encoding::Skiplist => "skiplist",
            Encoding::Btree => "btree",
            Encoding::StreamP => "stream",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn type_names_match_valkey() {
        assert_eq!(Type::String.name(), "string");
        assert_eq!(Type::List.name(), "list");
        assert_eq!(Type::Hash.name(), "hash");
        assert_eq!(Type::Set.name(), "set");
        assert_eq!(Type::ZSet.name(), "zset");
        assert_eq!(Type::Stream.name(), "stream");
    }

    #[test]
    fn encoding_names_match_valkey() {
        assert_eq!(Encoding::Int.name(), "int");
        assert_eq!(Encoding::Embstr.name(), "embstr");
        assert_eq!(Encoding::Raw.name(), "raw");
        assert_eq!(Encoding::Listpack.name(), "listpack");
        assert_eq!(Encoding::Quicklist.name(), "quicklist");
        assert_eq!(Encoding::Hashtable.name(), "hashtable");
        assert_eq!(Encoding::Intset.name(), "intset");
        assert_eq!(Encoding::Skiplist.name(), "skiplist");
        assert_eq!(Encoding::Btree.name(), "btree");
        assert_eq!(Encoding::StreamP.name(), "stream");
    }

    #[test]
    fn type_and_encoding_are_copy() {
        let t = Type::String;
        let t2 = t;
        assert_eq!(t, t2);
        let e = Encoding::Int;
        let e2 = e;
        assert_eq!(e, e2);
    }
}
