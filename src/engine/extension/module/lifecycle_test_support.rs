use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Drops {
    pub strings: usize,
    pub keys: usize,
    pub replies: usize,
}

thread_local! {
    static DROPS: Cell<Drops> = Cell::new(Drops::default());
}

pub(crate) fn snapshot() -> Drops {
    DROPS.with(Cell::get)
}

pub(crate) fn string_dropped() {
    DROPS.with(|drops| {
        let mut value = drops.get();
        value.strings += 1;
        drops.set(value);
    });
}

pub(crate) fn key_dropped() {
    DROPS.with(|drops| {
        let mut value = drops.get();
        value.keys += 1;
        drops.set(value);
    });
}

pub(crate) fn reply_dropped() {
    DROPS.with(|drops| {
        let mut value = drops.get();
        value.replies += 1;
        drops.set(value);
    });
}
