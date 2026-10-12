#[path = "../../../executors/src/eoa/worker/fee_math.rs"]
mod fee_math;

/// Independent 192-bit product/division reference: three base-2^64 limbs.
/// The production algorithm divides by 100 before multiplying; this reference
/// first forms the complete product and then performs long division by 100.
#[cfg(test)]
fn wide_reference(value: u128, multiplier: u32, cap: Option<u128>) -> u128 {
    let mask = u128::from(u64::MAX);
    let product_low = (value & mask) * u128::from(multiplier);
    let product_high = (value >> 64) * u128::from(multiplier) + (product_low >> 64);
    let high_limb = product_high >> 64;
    if high_limb >= 100 {
        return cap.unwrap_or(u128::MAX);
    }
    let upper = (high_limb << 64) | (product_high & mask);
    let quotient_high = upper / 100;
    let lower = ((upper % 100) << 64) | (product_low & mask);
    let quotient = (quotient_high << 64) | (lower / 100);
    quotient.min(cap.unwrap_or(u128::MAX))
}

#[cfg(kani)]
mod proofs {
    use super::*;

    #[kani::proof]
    fn all_multipliers_respect_caps_and_priority() {
        let fee: u128 = kani::any();
        let priority: u128 = kani::any();
        let multiplier: u32 = kani::any();
        let fee_cap: Option<u128> = kani::any();
        let priority_cap: Option<u128> = kani::any();
        let (new_fee, new_priority) =
            fee_math::capped_dynamic_fees(fee, priority, multiplier, fee_cap, priority_cap);
        assert!(new_fee <= fee_cap.unwrap_or(u128::MAX));
        assert!(new_priority <= priority_cap.unwrap_or(u128::MAX));
        assert!(new_priority <= new_fee);
    }

    #[kani::proof]
    fn all_increases_are_nondecreasing_when_cap_permits() {
        let value: u128 = kani::any();
        let multiplier: u32 = kani::any();
        let cap: u128 = kani::any();
        kani::assume(multiplier > 100);
        kani::assume(cap >= value);
        assert!(fee_math::capped_increase(value, multiplier, Some(cap)) >= value);
    }

    #[kani::proof]
    fn actual_parts_match_mathematical_saturation_threshold() {
        let quotient: u128 = kani::any();
        let remainder: u128 = kani::any();
        let cap: Option<u128> = kani::any();
        kani::assume(remainder < 100);
        // q*120 + floor(r*120/100) is floor(value*120/100) by
        // Euclidean division. Specify saturation by a pre-multiply boundary,
        // using no saturating operation in the reference.
        let fractional = remainder * 120 / 100;
        let result = fee_math::capped_increase_parts(quotient, remainder, 120, cap);
        if quotient > u128::MAX / 120
            || (quotient == u128::MAX / 120 && fractional > u128::MAX % 120)
        {
            assert_eq!(result, cap.unwrap_or(u128::MAX));
        } else {
            assert_eq!(
                result,
                (quotient * 120 + fractional).min(cap.unwrap_or(u128::MAX))
            );
        }
    }

    #[kani::proof]
    fn division_parts_reconstruct_all_inputs() {
        let value: u128 = kani::any();
        let (quotient, remainder) = fee_math::division_parts(value);
        assert!(remainder < 100);
        assert!(quotient <= u128::MAX / 100);
        assert!(quotient < u128::MAX / 100 || remainder <= u128::MAX % 100);
        assert_eq!(quotient * 100 + remainder, value);
    }

    #[kani::proof]
    fn actual_dynamic_bump_preserves_valid_order_and_values() {
        let fee: u128 = kani::any();
        let priority: u128 = kani::any();
        let fee_cap: u128 = kani::any();
        let priority_cap: u128 = kani::any();
        kani::assume(priority <= fee);
        kani::assume(fee_cap >= fee);
        kani::assume(priority_cap >= priority);
        let (new_fee, new_priority) =
            fee_math::capped_dynamic_fees(fee, priority, 120, Some(fee_cap), Some(priority_cap));
        assert!(new_fee >= fee);
        assert!(new_priority >= priority);
        assert!(new_priority <= new_fee);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wide_reference_boundary_examples() {
        for value in [
            0,
            1,
            99,
            100,
            101,
            u128::MAX / 120,
            u128::MAX / 120 + 1,
            u128::MAX / 6 * 5,
            u128::MAX,
        ] {
            for multiplier in [0, 1, 100, 120, u32::MAX] {
                for cap in [None, Some(0), Some(7), Some(u128::MAX)] {
                    assert_eq!(
                        fee_math::capped_increase(value, multiplier, cap),
                        wide_reference(value, multiplier, cap)
                    );
                }
            }
        }
        assert_eq!(wide_reference(u128::MAX, 0, None), 0);
        assert_eq!(wide_reference(u128::MAX, 100, None), u128::MAX);
        assert_eq!(wide_reference(u128::MAX, 120, None), u128::MAX);
        assert_eq!(wide_reference(u128::MAX, u32::MAX, Some(7)), 7);
        assert_eq!(wide_reference(101, 120, None), 121);
        assert_eq!(wide_reference(99, 120, None), 118);
    }
}
