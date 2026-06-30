# Geyser Fast Path Optimizations

This document records the local performance branch changes for later comparison,
rebasing, and selective merging into other Yellowstone gRPC branches.

Branch context:

- Current working branch: `optimize_v14.0.0+solana.4.1.0`
- Ported from commit: `cfaf209f18103170a346fa45974fe52a85cde083`
- Original fast-path branch: `codex-geyser-performance-fastpath`
- Original reference commit for comparison: `8f11ed8321f4d646341b1c37ecb7a01097fc0dfd`
- Target workload: self-use Geyser plugin deployment
- Target subscriptions: `accounts`, `transactions`, and `blocks_meta`
- Target commitment: `Processed`
- Non-goals for this branch: block reconstruction, slot replay, generic public API parity,
  and `token_accounts` fast-path support

v14 port notes:

- Preserve `SubscribeDeshred` / `DeshredFilter` behavior; deshred updates stay on the
  independent deshred broadcast path.
- Preserve v14 `foldhash` data structures in filter/message hot paths.
- Preserve v14 block-machine ordering: block meta and commitment slot messages are fed to
  reconstruction after ordinary processed events when reconstruction is enabled.

## Operating Assumptions

This branch is optimized under these assumptions:

- `grpc.replay_stored_slots = 0`
- Subscribers do not request historical replay through `from_slot`.
- Subscribers do not rely on reconstructed `Block` updates.
- Subscribers do not use transaction `token_accounts` owner expansion.
- Account early filtering only needs account pubkey, owner, cuckoo account filter, and
  `nonempty_txn_signature` checks.
- Full account and transaction filtering still runs after broadcast, so early filters may
  conservatively pass extra messages but must not drop messages required by the target
  workload.

If any of these assumptions change, review the "Known Limits" section before merging.

## Config Changes

### `processed_messages_max`

File: `yellowstone-grpc-geyser/src/config.rs`

Adds `grpc.processed_messages_max`, defaulting to `31`.

Purpose:

- Allows processed message batching to be tuned without recompiling.
- Keeps the old default behavior when the option is omitted.
- Values are clamped to at least `1` before use.

Used in:

- `GrpcService::create`
- `GrpcService::geyser_processed_loop`

### `strip_transaction_log_messages`

File: `yellowstone-grpc-geyser/src/config.rs`

Adds `grpc.strip_transaction_log_messages`, defaulting to `false`.

Purpose:

- Allows streamed `TransactionStatusMeta.log_messages` to be dropped.
- Reduces payload size and encoding work when logs are not needed.

Implemented through:

- `create_transaction_meta_with_config(meta, strip_log_messages)`
- `MessageTransaction::from_geyser_with_config`
- `MessageTransaction::from_geyser_with_account_keys_config`

## Reconstruction And Replay Bypass

Files:

- `yellowstone-grpc-geyser/src/grpc.rs`
- `yellowstone-grpc-geyser/src/plugin/entry.rs`

The branch uses `replay_stored_slots == 0` as the single switch for bypassing block
reconstruction and replay storage.

Important behavior:

- When `replay_stored_slots == 0`, the block reconstruction loop is not spawned.
- No replay request channel is created.
- `from_slot` requests return `from_slot is not supported`.
- `geyser_processed_loop` broadcasts processed batches directly.
- `geyser_loop` remains the reconstruction path and is not parameterized by the bypass flag.
- Confirmed/finalized block-reconstruction side broadcasts are skipped.
- `blocks_meta_tx_for_fast_path` was intentionally not kept; block meta delivery continues
  through the main `grpc_channel -> geyser_processed_loop -> broadcast -> FilterBlocksMeta`
  path when reconstruction is bypassed.

Reasoning:

- This deployment only consumes `Processed` updates.
- Avoiding reconstruction removes a large amount of work from the hot path.
- `replay_stored_slots` already captures whether replay state should exist, so an extra
  `reconstruction_bypass` config was removed to avoid duplicate knobs.

Merge note:

- If a target branch needs `Confirmed`, `Finalized`, reconstructed `Block`, or replay,
  keep `replay_stored_slots > 0` and do not assume the bypass path is safe for that branch.

## Early Account Filtering

Files:

- `yellowstone-grpc-geyser/src/plugin/entry.rs`
- `yellowstone-grpc-geyser/src/plugin/filter/filter.rs`
- `yellowstone-grpc-geyser/src/plugin/message.rs`

The plugin now maintains an `AccountFilterGate` shared between gRPC subscription updates and
Geyser account notifications.

Hot-path behavior:

- Parse account pubkey and owner once in `update_account`.
- Drop non-startup account notifications before message construction when no active account
  rule matches.
- Reuse parsed pubkey and owner when building `MessageAccount`.

The early account rule checks:

- `nonempty_txn_signature`
- explicit account pubkey set
- cuckoo account filter
- owner pubkey set

The early account rule intentionally does not check:

- memcmp filters
- datasize filters
- token account state filters
- lamports filters

Reasoning:

- The target workload does not use these account state filters for early rejection.
- Removing `FilterAccountsState` from the early rule avoids passing account data/lamports
  through the hot gate and avoids a per-rule branch.
- Full account filtering still runs in `FilterAccounts::get_updates`, so this is
  conservative: it can pass extra accounts, but under normal subscription semantics it does
  not drop accounts that full filtering would accept.

`filter_limits` note:

- `PluginInner.filter_limits` is retained for config ownership parity.
- The previous `update_account` owner reject check was removed from the hot path because it
  duplicated subscription filter behavior for this workload.

## Early Transaction Filtering

Files:

- `yellowstone-grpc-geyser/src/plugin/entry.rs`
- `yellowstone-grpc-geyser/src/plugin/filter/filter.rs`
- `yellowstone-grpc-geyser/src/plugin/message.rs`

The plugin now maintains a `TransactionFilterGate` shared between gRPC subscription updates
and Geyser transaction notifications.

Hot-path behavior:

- `notify_transaction` calls `TransactionFilterGate::allows_and_account_keys`.
- The gate first matches using `TxAccountKeysView`, which borrows:
  - static transaction account keys
  - loaded writable addresses
  - loaded readonly addresses
- A `HashSet<Pubkey>` is only created after a transaction passes the early gate.
- The returned account-key set is reused when constructing `MessageTransaction`.

Reasoning:

- The earlier implementation built a `HashSet` before deciding whether to drop the
  transaction.
- `TxAccountKeysView` avoids allocation and hashing on dropped transactions.
- This is especially useful when most transactions are rejected by account filters.

The early transaction rule checks:

- `vote`
- `failed`
- `signature`
- `account_include`
- `account_exclude`
- `account_required`

The early transaction rule intentionally does not check:

- `token_accounts`

See "Known Limits" for the resulting constraint.

## Early Deshred Transaction Filtering

Files:

- `yellowstone-grpc-geyser/src/plugin/entry.rs`
- `yellowstone-grpc-geyser/src/plugin/filter/filter.rs`
- `yellowstone-grpc-geyser/src/grpc.rs`

The plugin now maintains a `DeshredTransactionFilterGate` shared between
`SubscribeDeshred` filter updates and Geyser deshred transaction notifications.

Hot-path behavior:

- `notify_deshred_transaction` checks the gate before constructing
  `MessageDeshredTransaction`.
- The gate borrows static transaction account keys plus loaded writable/readonly addresses
  directly from the Geyser deshred transaction info.
- Deshred transaction conversion, allocation, and broadcast are skipped when no active
  deshred transaction rule matches.
- Slot updates on the deshred channel are unaffected.

The early deshred transaction rule checks:

- `vote`
- `account_include`
- `account_exclude`
- `account_required`

The full deshred output filter now also uses the same borrowed account-key view, avoiding a
temporary `HashSet<&Pubkey>` for `account_required`.

## Gate Storage

Files:

- `yellowstone-grpc-geyser/src/plugin/filter/filter.rs`
- `yellowstone-grpc-geyser/src/grpc.rs`

Account, transaction, and deshred transaction gates use:

- `StdMutex<HashMap<usize, Vec<Rule>>>` for subscription updates
- `ArcSwap<Vec<Rule>>` for lock-free hot-path reads

Update path:

- Main gRPC subscription updates build a `Filter`.
- `FilterGateGuard::update` installs account and transaction rules for that client id.
- `SubscribeDeshred` updates build a `DeshredFilter`.
- `DeshredFilterGateGuard::update` installs deshred transaction rules for that client id.
- Guard `drop` removes the client rules on disconnect.
- Rebuilding the merged rule vector precomputes `total_rules` and uses
  `Vec::with_capacity(total_rules)`.

Read path:

- Geyser notification code reads the current merged rules through `ArcSwap::load`.
- No mutex is taken on account, transaction, or deshred transaction notification hot paths.

Comparison with `8f11ed8`:

- This branch keeps the lock-free `ArcSwap::load` hot read style.
- This branch restores the `allows_and_account_keys` shape from `8f11ed8` for transactions.
- The v14 port keeps v14's smaller `Vec<Pubkey>` transaction rule representation instead of
  adding duplicate `Vec` and `HashSet` forms.

## Token Accounts Limit

The full transaction filter still supports `token_accounts` owner expansion in
`FilterTransactionsInner`.

The early transaction gate does not support `token_accounts`.

Impact:

- Under the target workload, this is acceptable because `token_accounts` is not used.
- If a subscriber uses `token_accounts` and expects a token-balance owner to match even when
  the owner is not in transaction account keys, the early gate can drop that transaction
  before full filtering sees it.

Merge options for branches that need generic behavior:

1. Reject `token_accounts` subscriptions when early filtering is enabled.
2. Treat rules with `token_accounts` as early-gate pass-through.
3. Reintroduce token owner expansion into the early gate.

For this self-use branch, option 1 is the safest future hardening if config validation is
desired.

## Block Meta Path

Block meta notifications are not sent through a special side channel in this branch.

Current path:

```text
notify_block_metadata
  -> grpc_channel
  -> geyser_processed_loop
  -> broadcast Processed
  -> FilterBlocksMeta::get_updates
  -> subscriber
```

This works even when unary gRPC methods are disabled. `blocks_meta_tx` is only for unary
block meta cache storage, not for streaming subscribers.

## Differences From `8f11ed8`

Kept from `8f11ed8`:

- Early account and transaction gates.
- Transaction `allows_and_account_keys` style.
- Drop transactions before building a full `MessageTransaction` when possible.
- Pass matching account keys into message construction to avoid recomputing them.

Changed from `8f11ed8`:

- Gate storage uses the current branch's `StdMutex + ArcSwap<Vec<Rule>>` structure.
- Hot reads use `ArcSwap::load`.
- `TransactionFilterRule` stores v14-style `Vec<Pubkey>` fields and matches them through
  `TxAccountKeysView`.
- `token_accounts` is omitted from the transaction early gate.
- Account early gate omits `FilterAccountsState`.
- Reconstruction bypass is controlled only by `replay_stored_slots == 0`.
- `strip_transaction_log_messages` is configurable.
- `SubscribeDeshred` support is preserved and remains independent of the ordinary
  transaction fast path.

## Files Touched

- `Cargo.lock`
- `yellowstone-grpc-geyser/Cargo.toml`
- `yellowstone-grpc-geyser/src/config.rs`
- `yellowstone-grpc-geyser/src/grpc.rs`
- `yellowstone-grpc-geyser/src/plugin/convert_to.rs`
- `yellowstone-grpc-geyser/src/plugin/entry.rs`
- `yellowstone-grpc-geyser/src/plugin/filter/filter.rs`
- `yellowstone-grpc-geyser/src/plugin/filter/mod.rs`
- `yellowstone-grpc-geyser/src/plugin/message.rs`

## Merge Checklist

Before merging these changes into another branch, confirm:

- The target branch is allowed to run with `replay_stored_slots = 0`.
- The target deployment does not need `from_slot` replay.
- The target deployment does not need reconstructed `Block` updates.
- The target deployment only requires `Processed` streaming behavior.
- The target deployment does not need ordinary transaction early filtering to affect
  `SubscribeDeshred`; deshred remains a separate stream.
- Subscribers do not use `token_accounts`, or the target branch adds one of the merge options
  from "Token Accounts Limit".
- Account filters that rely only on memcmp/datasize/lamports/token account state can tolerate
  early gate pass-through before full filtering.
- `strip_transaction_log_messages` is set only when transaction logs are not needed.
- `processed_messages_max` is tuned for the deployment's latency/throughput tradeoff.

## Verification Performed

Commands run on this branch:

```bash
cargo fmt
cargo test -p yellowstone-grpc-geyser -- --nocapture
git diff --check
```

Observed result:

- `cargo test -p yellowstone-grpc-geyser -- --nocapture` passed with 101 tests on the
  v14 port.
- `git diff --check` passed.
- `cargo fmt` ran successfully, with existing stable rustfmt warnings about nightly-only
  `imports_granularity` and `group_imports` options.

Not verified:

- No production traffic benchmark was run.
- No flamegraph or allocation profile was captured.
- No clippy run was performed.

## Follow-Up Optimization Candidates

Low-risk candidates:

- Reject `token_accounts` subscriptions when early filtering is enabled, so the unsupported
  fast-path combination fails explicitly.
- If cuckoo filters are never used in this deployment, remove or bypass cuckoo matching from
  the account early gate.

Benchmark-dependent candidates:

- Compare `TxAccountKeysView` against prebuilt `HashSet` for pass-heavy workloads.
- Add small-set specialization for transaction account include/exclude/required filters.
- Reuse allocation buffers for processed message batches if profiling shows allocation churn.
