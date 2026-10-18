use std::cell::Cell;

thread_local! {
    static STATE: Cell<u64> = const { Cell::new(0x9e37_79b9_7f4a_7c15) };
}

pub fn random_usize(max: usize) -> usize {
    if max == 0 {
        return 0;
    }
    STATE.with(|state| {
        let next = state
            .get()
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        state.set(next);
        (next % max as u64) as usize
    })
}

pub fn shuffle<T>(slice: &mut [T]) {
    for index in (1..slice.len()).rev() {
        slice.swap(index, random_usize(index + 1));
    }
}
