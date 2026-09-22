use {
    super::*,
    crate::{
        plugin::{filter::fixtures, message::MessageAccount},
        util::stream::load_aware_channel,
    },
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    yellowstone_grpc_proto::prelude::{SubscribeRequest, SubscribeRequestFilterAccounts},
};

fn account_filter(key: Pubkey) -> Filter {
    Filter::new(
        &SubscribeRequest {
            accounts: HashMap::from([(
                "account".into(),
                SubscribeRequestFilterAccounts {
                    account: vec![key.to_string()],
                    ..Default::default()
                },
            )]),
            ..Default::default()
        },
        &FilterLimits::default(),
        &mut fixtures::filter_names(),
    )
    .unwrap()
}

#[tokio::test]
async fn processed_loop_preserves_order_bank_ids_and_lazy_encoding() {
    let (tx, rx) = mpsc::unbounded_channel();
    let broadcast = SubscriberChannels::new(16, 16, 16);
    let mut processed = broadcast.subscribe(CommitmentLevel::Processed);
    let mut confirmed = broadcast.subscribe(CommitmentLevel::Confirmed);
    let mut finalized = broadcast.subscribe(CommitmentLevel::Finalized);
    let account = fixtures::simple_message_account(Pubkey::new_unique(), Pubkey::new_unique());
    let transaction =
        fixtures::message_transaction(Signature::default(), vec![], false, Default::default());
    let mut fork_slot = fixtures::message_slot(100, SlotStatus::Processed);
    fork_slot.bank_id = Some(999);
    let messages = vec![
        Message::Account(Arc::clone(&account)),
        Message::Transaction(Arc::clone(&transaction)),
        Message::Slot(Arc::new(fork_slot)),
        Message::BlockMeta(fixtures::message_block_meta(100)),
        Message::Entry(fixtures::message_entry(100, 0)),
        Message::BlockFooter(fixtures::message_block_footer(100, 999)),
        Message::Slot(Arc::new(fixtures::message_slot(100, SlotStatus::Finalized))),
    ];
    for message in &messages {
        tx.send(message.clone()).unwrap();
    }
    let handle = tokio::spawn(GrpcService::geyser_processed_loop(
        BatchStreamUnboundedReceiver::new(rx),
        broadcast,
    ));
    let mut received = Vec::new();
    for expected_len in [4, 3] {
        let batch = tokio::time::timeout(Duration::from_secs(2), processed.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(batch.len(), expected_len);
        received.extend(batch.iter().cloned());
    }
    assert_eq!(received, messages);
    assert!(account.account.pre_encoded.get().is_none());
    assert!(transaction.transaction.pre_encoded.get().is_none());
    assert!(confirmed.try_recv().is_err());
    assert!(finalized.try_recv().is_err());
    // An isolated update must flush while the input channel remains open.
    tx.send(messages[0].clone()).unwrap();
    let batch = tokio::time::timeout(Duration::from_secs(2), processed.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(batch.as_slice(), &messages[..1]);
    drop(tx);
    handle.await.unwrap();
}

#[tokio::test]
async fn processed_loop_splits_at_4_and_drains_on_close() {
    let (tx, rx) = mpsc::unbounded_channel();
    let broadcast = SubscriberChannels::new(16, 16, 16);
    let mut processed = broadcast.subscribe(CommitmentLevel::Processed);
    for slot in 1..=5 {
        tx.send(Message::Slot(Arc::new(fixtures::message_slot(
            slot,
            SlotStatus::Processed,
        ))))
        .unwrap();
    }
    drop(tx);
    GrpcService::geyser_processed_loop(BatchStreamUnboundedReceiver::new(rx), broadcast).await;
    let first = processed.recv().await.unwrap();
    let second = processed.recv().await.unwrap();
    assert_eq!(first.len(), 4);
    assert_eq!(second.len(), 1);
    assert!(matches!(&second[0], Message::Slot(slot) if slot.slot == 5));
}

#[tokio::test]
async fn live_filter_coalescing_and_cancellation_update_gates() {
    let keys = fixtures::deterministic_pubkeys(72, 3);
    let gates = FastPathFilterGates::default();
    let ct = CancellationToken::new();
    let mut session = ClientSession::new(1, None, "test".into(), ct.clone(), None);
    session.fast_path_gates = Some(gates.clone());
    let (client_tx, client_rx) = mpsc::unbounded_channel();
    let (stream_tx, _stream_rx) = load_aware_channel(16);
    client_tx
        .send(Some((None, account_filter(keys[0]))))
        .unwrap();
    client_tx
        .send(Some((None, account_filter(keys[1]))))
        .unwrap();
    let handle = tokio::spawn(GrpcService::client_loop(
        session,
        stream_tx,
        client_rx,
        None,
        SubscriberChannels::new(16, 16, 16),
        None,
        TaskTracker::new(),
    ));
    tokio::time::timeout(Duration::from_secs(2), async {
        while !gates.allows_account(&keys[1], &keys[2], false) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!gates.allows_account(&keys[0], &keys[2], false));
    ct.cancel();
    handle.await.unwrap();
    assert!(!gates.allows_account(&keys[1], &keys[2], false));
}

#[tokio::test]
async fn snapshot_filter_registers_live_gate_and_cleans_up() {
    let key = Pubkey::new_unique();
    let gates = FastPathFilterGates::default();
    let ct = CancellationToken::new();
    let mut session = ClientSession::new(1, None, "snapshot".into(), ct.clone(), None);
    session.fast_path_gates = Some(gates.clone());
    let (client_tx, mut client_rx) = mpsc::unbounded_channel();
    let (stream_tx, mut stream_rx) = load_aware_channel(16);
    let (snapshot_tx, snapshot_rx) = crossbeam_channel::unbounded();
    let account = MessageAccount {
        is_startup: true,
        bank_id: None,
        ..(*fixtures::simple_message_account(key, key)).clone()
    };
    snapshot_tx
        .send(Box::new(Message::Account(Arc::new(account))))
        .unwrap();
    drop(snapshot_tx);
    client_tx.send(Some((None, account_filter(key)))).unwrap();
    GrpcService::client_loop_snapshot(&mut session, stream_tx, &mut client_rx, snapshot_rx, ct)
        .await
        .unwrap();
    assert!(stream_rx.recv().await.unwrap().is_ok());
    assert!(gates.allows_account(&key, &key, false));
    drop(session);
    assert!(!gates.allows_account(&key, &key, false));
}

#[tokio::test]
async fn startup_selects_fast_path_only_for_zero_replay() {
    for replay in [0, 150] {
        let config: ConfigGrpc = serde_json::from_value(serde_json::json!({
            "address": "127.0.0.1:0", "replay_stored_slots": replay,
            "unary_disabled": true, "channel_capacity": 16
        }))
        .unwrap();
        let ct = CancellationToken::new();
        let tracker = TaskTracker::new();
        let (tx, rx) = mpsc::unbounded_channel();
        let result = GrpcService::create(
            config,
            true,
            ct.clone(),
            tracker.clone(),
            Arc::new(FileWatcher::new().unwrap()),
            BatchStreamUnboundedReceiver::new(rx),
        )
        .await
        .unwrap();
        assert_eq!(result.block_reconstruction_tx.is_none(), replay == 0);
        assert_eq!(result.fast_path_gates.is_some(), replay == 0);
        assert!(result.blocks_meta_tx.is_none());
        if replay == 0 {
            let mut processed = result.broadcast.subscribe(CommitmentLevel::Processed);
            tx.send(Message::BlockMeta(fixtures::message_block_meta(42)))
                .unwrap();
            let batch = tokio::time::timeout(Duration::from_secs(2), processed.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(matches!(&batch[0], Message::BlockMeta(meta) if meta.block_meta.slot == 42));
        }
        drop(tx);
        drop(result);
        ct.cancel();
        tracker.close();
        tokio::time::timeout(Duration::from_secs(2), tracker.wait())
            .await
            .unwrap();
    }
}

#[test]
fn deshred_session_updates_and_drop_remove_only_its_rules() {
    use {
        agave_geyser_plugin_interface::geyser_plugin_interface::{
            ReplicaDeshredTransactionInfo, ReplicaDeshredTransactionInfoVersions,
        },
        solana_transaction::versioned::VersionedTransaction,
        yellowstone_grpc_proto::prelude::{
            SubscribeDeshredRequest, SubscribeRequestFilterDeshredTransactions,
        },
    };
    let gate = FastPathFilterGates::default();
    let transaction = VersionedTransaction::default();
    let signature = Signature::default();
    let info = ReplicaDeshredTransactionInfo {
        signature: &signature,
        is_vote: false,
        transaction: &transaction,
        loaded_addresses: None,
    };
    let info = ReplicaDeshredTransactionInfoVersions::V0_0_1(&info);
    let make_filter = |vote| {
        DeshredFilter::new(
            &SubscribeDeshredRequest {
                deshred_transactions: HashMap::from([(
                    "tx".into(),
                    SubscribeRequestFilterDeshredTransactions {
                        vote: Some(vote),
                        ..Default::default()
                    },
                )]),
                ..Default::default()
            },
            &FilterLimits::default(),
            &mut fixtures::filter_names(),
        )
        .unwrap()
    };
    let mut session = DeshredClientSession::new(1, None, CancellationToken::new(), None);
    session.fast_path_gates = Some(gate.clone());
    session.set_filter(make_filter(false));
    assert!(gate.allows_deshred(&info));
    session.set_filter(make_filter(true));
    assert!(!gate.allows_deshred(&info));
    gate.update_deshred_client(2, &make_filter(false));
    drop(session);
    assert!(gate.allows_deshred(&info));
    gate.remove_deshred_client(2);
    assert!(!gate.allows_deshred(&info));
}
