-- Track when a gas wallet was last activated (assigned to a peer).
--
-- The reclaim grace period must be measured from the last activation, not from
-- row creation: pool wallets are recycled, so `created_at` is stale by the time
-- a wallet is re-activated and the grace guard never fired for them.
ALTER TABLE gas_wallets ADD COLUMN activated_at TIMESTAMP;

-- Backfill in-use wallets so they are treated as established (past the grace
-- window) rather than freshly activated. Inactive wallets keep NULL.
UPDATE gas_wallets SET activated_at = created_at WHERE status != 'inactive';
