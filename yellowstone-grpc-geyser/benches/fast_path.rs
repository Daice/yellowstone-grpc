//! Callback-only comparison: construction without a gate versus gate + accepted construction.
//! Does not measure reconstruction, encoding, queues, network or validator scheduling.
use {
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaTransactionInfoV3,
    },
    criterion::{criterion_group, criterion_main, Criterion},
    solana_hash::Hash,
    solana_message::{legacy::Message as LegacyMessage, v0::LoadedAddresses, VersionedMessage},
    solana_signature::Signature,
    solana_transaction::versioned::VersionedTransaction,
    solana_transaction_status::TransactionStatusMeta,
    std::{collections::HashMap, hint::black_box, time::Duration},
    yellowstone_grpc_geyser::plugin::{
        filter::{fixtures, limits::FilterLimits, FastPathFilterGates, Filter},
        message::{MessageAccount, MessageTransaction},
    },
    yellowstone_grpc_proto::geyser::{
        SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterTransactions,
    },
};

fn gates(request: SubscribeRequest) -> FastPathFilterGates {
    let filter = Filter::new(
        &request,
        &FilterLimits::default(),
        &mut fixtures::filter_names(),
    )
    .unwrap();
    let gates = FastPathFilterGates::default();
    gates.update_client(1, &filter);
    gates
}

fn callback_cost(c: &mut Criterion) {
    let keys = fixtures::deterministic_pubkeys(81, 64);
    let watched = fixtures::deterministic_pubkeys(82, 10_000);
    let signature = Signature::default();
    let hash = Hash::default();
    let tx = VersionedTransaction {
        signatures: vec![signature],
        message: VersionedMessage::Legacy(LegacyMessage {
            account_keys: keys[..32].to_vec(),
            ..Default::default()
        }),
    };
    let meta = TransactionStatusMeta {
        loaded_addresses: LoadedAddresses {
            writable: keys[32..48].to_vec(),
            readonly: keys[48..].to_vec(),
        },
        ..Default::default()
    };
    let info = ReplicaTransactionInfoV3 {
        signature: &signature,
        message_hash: &hash,
        is_vote: false,
        transaction: &tx,
        transaction_status_meta: &meta,
        index: 0,
    };
    let mut tx_group = c.benchmark_group("fast_path/transaction_64_keys");
    tx_group.bench_function("construct_without_gate", |b| {
        b.iter(|| black_box(MessageTransaction::from_geyser(black_box(&info), 10, 77)))
    });
    for size in [0, 1, 10_000] {
        for matched in [false, true] {
            if size == 0 && !matched {
                continue;
            }
            let mut include = watched[..size].to_vec();
            if size > 0 && matched {
                include[0] = keys[63];
            }
            let gate = gates(SubscribeRequest {
                transactions: HashMap::from([(
                    "tx".into(),
                    SubscribeRequestFilterTransactions {
                        account_include: include.iter().map(ToString::to_string).collect(),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            });
            let name = if matched { "hit" } else { "miss" };
            tx_group.bench_function(format!("gate_{name}_{size}"), |b| {
                b.iter(|| {
                    let keys = gate.transaction_account_keys(black_box(&info));
                    black_box(keys.map(|keys| {
                        MessageTransaction::from_geyser_with_account_keys(&info, 10, 77, keys)
                    }))
                })
            });
        }
    }
    for count in [1, 8, 64] {
        let gate = gates(SubscribeRequest {
            transactions: HashMap::from([(
                "tx".into(),
                SubscribeRequestFilterTransactions {
                    account_required: keys[..count].iter().map(ToString::to_string).collect(),
                    ..Default::default()
                },
            )]),
            ..Default::default()
        });
        tx_group.bench_function(format!("gate_required_hit_{count}"), |b| {
            b.iter(|| {
                let keys = gate.transaction_account_keys(black_box(&info));
                black_box(keys.map(|keys| {
                    MessageTransaction::from_geyser_with_account_keys(&info, 10, 77, keys)
                }))
            })
        });
    }
    tx_group.finish();

    let data = vec![7; 4096];
    let account = ReplicaAccountInfoV3 {
        pubkey: keys[0].as_ref(),
        lamports: 1,
        owner: keys[1].as_ref(),
        executable: false,
        rent_epoch: 0,
        data: &data,
        write_version: 1,
        txn: None,
    };
    let mut account_group = c.benchmark_group("fast_path/account_4k");
    account_group.bench_function("construct_without_gate", |b| {
        b.iter(|| {
            black_box(MessageAccount::from_geyser_for_bank(
                black_box(&account),
                10,
                77,
                keys[0],
                keys[1],
            ))
        })
    });
    for matched in [false, true] {
        let key = if matched { keys[0] } else { watched[0] };
        let gate = gates(SubscribeRequest {
            accounts: HashMap::from([(
                "account".into(),
                SubscribeRequestFilterAccounts {
                    account: vec![key.to_string()],
                    ..Default::default()
                },
            )]),
            ..Default::default()
        });
        account_group.bench_function(if matched { "gate_hit" } else { "gate_miss" }, |b| {
            b.iter(|| {
                black_box(
                    gate.allows_account(black_box(&keys[0]), &keys[1], false)
                        .then(|| {
                            MessageAccount::from_geyser_for_bank(&account, 10, 77, keys[0], keys[1])
                        }),
                )
            })
        });
    }
    account_group.finish();
}

criterion_group! { name = benches; config = Criterion::default().sample_size(30).warm_up_time(Duration::from_secs(1)).measurement_time(Duration::from_secs(2)); targets = callback_cost }
criterion_main!(benches);
