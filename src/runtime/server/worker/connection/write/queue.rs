use super::{
    OutputFrame, WriteOutcome, MAX_IOV_PER_WRITE, NET_MAX_WRITES_PER_EVENT,
    REPLY_BUFFER_POOL_MAX_CAP, REPLY_BUFFER_POOL_MAX_LEN,
};
use std::collections::VecDeque;
use std::io::{IoSlice, Write};

/// Match C writevToClient: stop after the first whole frame crossing the
/// per-event threshold, allowing a single large reply to exceed the budget.
pub(super) fn c_client_write_budget(queue: &VecDeque<OutputFrame>) -> usize {
    let mut total = 0usize;
    for frame in queue.iter().take(MAX_IOV_PER_WRITE) {
        total = total.saturating_add(frame.len());
        if total > NET_MAX_WRITES_PER_EVENT {
            break;
        }
    }
    total
}

pub(super) fn flush_queue_vectored(
    stream: &mut impl Write,
    queue: &mut VecDeque<OutputFrame>,
    byte_budget: Option<usize>,
    mut reply_buffer_pool: Option<&mut Vec<Vec<u8>>>,
) -> (WriteOutcome, usize) {
    let mut total_written = 0usize;
    let mut released_allocation_bytes = 0usize;
    loop {
        if queue.is_empty() {
            return (WriteOutcome::Complete, released_allocation_bytes);
        }
        let remaining_budget = byte_budget
            .map(|budget| budget.saturating_sub(total_written))
            .unwrap_or(usize::MAX);
        if remaining_budget == 0 {
            return (WriteOutcome::BudgetExhausted, released_allocation_bytes);
        }
        let (total, nwritten) = {
            let mut iov_buf = [IoSlice::new(&[]); MAX_IOV_PER_WRITE];
            let mut total = 0usize;
            let mut nslices = 0usize;
            for chunk in queue.iter().take(MAX_IOV_PER_WRITE) {
                let available = remaining_budget.saturating_sub(total);
                if available == 0 {
                    break;
                }
                let included = chunk.len().min(available);
                iov_buf[nslices] = IoSlice::new(&chunk[..included]);
                total += included;
                nslices += 1;
            }
            match stream.write_vectored(&iov_buf[..nslices]) {
                // Keep unsent frames on failure: dropping them here would
                // reopen diskless backpressure before role-aware teardown.
                Ok(0) => {
                    return (
                        WriteOutcome::Failed(std::io::ErrorKind::WriteZero.into()),
                        released_allocation_bytes,
                    );
                }
                Ok(n) => (total, n),
                Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    return (WriteOutcome::Backpressured, released_allocation_bytes);
                }
                Err(error) => return (WriteOutcome::Failed(error), released_allocation_bytes),
            }
        };
        total_written = total_written.saturating_add(nwritten);
        let mut n = nwritten;
        while n > 0 {
            let Some(front) = queue.front() else {
                break;
            };
            let flen = front.len();
            if n >= flen {
                let frame = queue.pop_front().expect("front exists");
                released_allocation_bytes =
                    released_allocation_bytes.saturating_add(frame.owned_allocation_bytes());
                // Only owned command buffers enter the bounded reply pool.
                if let OutputFrame::Owned(mut data) = frame {
                    if let Some(pool) = reply_buffer_pool.as_deref_mut() {
                        if data.capacity() <= REPLY_BUFFER_POOL_MAX_CAP
                            && pool.len() < REPLY_BUFFER_POOL_MAX_LEN
                        {
                            data.clear();
                            pool.push(data);
                        }
                    }
                }
                n -= flen;
            } else {
                queue.front_mut().expect("front exists").advance(n);
                break;
            }
        }
        if nwritten < total {
            return (WriteOutcome::Backpressured, released_allocation_bytes);
        }
        if byte_budget.is_some_and(|budget| total_written >= budget) && !queue.is_empty() {
            return (WriteOutcome::BudgetExhausted, released_allocation_bytes);
        }
    }
}
