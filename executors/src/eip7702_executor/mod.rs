pub mod confirm;
pub mod delegation_cache;
pub mod send;

/// Bundler queue IDs do not prove which admitted UID/calls a finalized receipt
/// executed. Keep this capability closed until independent on-chain attribution
/// is implemented; direct EOA type-4 transactions have a separate exact-wire gate.
pub const BUNDLED_EXECUTION_DISABLED_REASON: &str = "Bundled EIP7702 execution is disabled: on-chain attribution of the admitted UID and calls is not qualified. Reconcile existing jobs manually; use independently verified EOA execution for new work.";

pub fn require_bundled_execution_qualification() -> Result<(), engine_core::error::EngineError> {
    Err(engine_core::error::EngineError::ValidationError {
        message: BUNDLED_EXECUTION_DISABLED_REASON.into(),
    })
}
