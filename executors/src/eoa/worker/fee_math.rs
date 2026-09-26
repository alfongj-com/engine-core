//! Dependency-free fee arithmetic shared by transaction construction and Kani.

/// `min(cap, u128::MAX, floor(value * multiplier / 100))`, where the
/// multiplication in this specification is over mathematical natural numbers.
pub(super) fn capped_increase(value: u128, multiplier: u32, cap: Option<u128>) -> u128 {
    let (quotient, remainder) = division_parts(value);
    capped_increase_parts(quotient, remainder, multiplier, cap)
}

pub(super) fn division_parts(value: u128) -> (u128, u128) {
    (value / 100, value % 100)
}

/// Quotient/remainder form used by `capped_increase`. The caller supplies
/// `quotient = value / 100` and `remainder = value % 100`.
pub(super) fn capped_increase_parts(
    quotient: u128,
    remainder: u128,
    multiplier: u32,
    cap: Option<u128>,
) -> u128 {
    // Divide first without losing the remainder: multiplying u128 fee data
    // directly can wrap in release builds or panic in debug builds.
    let multiplier = u128::from(multiplier);
    let bumped = quotient
        .saturating_mul(multiplier)
        .saturating_add(remainder * multiplier / 100);
    bumped.min(cap.unwrap_or(u128::MAX))
}

/// Increase both dynamic fees, preserving independent caller caps and the
/// EIP-1559 requirement that the priority fee does not exceed the total fee.
pub(super) fn capped_dynamic_fees(
    fee: u128,
    priority: u128,
    multiplier: u32,
    fee_cap: Option<u128>,
    priority_cap: Option<u128>,
) -> (u128, u128) {
    let fee = capped_increase(fee, multiplier, fee_cap);
    let priority = capped_increase(priority, multiplier, priority_cap).min(fee);
    (fee, priority)
}
