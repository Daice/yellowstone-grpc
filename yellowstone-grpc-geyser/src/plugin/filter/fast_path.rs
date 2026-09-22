//! Callback gates for the self-use Processed path. Client filters remain authoritative.
use {
    super::{DeshredFilter, Filter, FilterAccountAggregate, FilterTransactionsInner},
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaDeshredTransactionInfoVersions, ReplicaTransactionInfoV3,
    },
    arc_swap::ArcSwap,
    foldhash::HashSet,
    solana_pubkey::Pubkey,
    solana_signature::Signature,
    std::{
        collections::HashMap,
        sync::{Arc, Mutex},
    },
    yellowstone_grpc_proto::cuckoo::CuckooFilter,
};

trait GateRule {
    fn is_unconditional(&self) -> bool;
}

/// Only subscription changes take the mutex. Merging copies rule handles, not large key sets.
#[derive(Debug)]
struct Gate<R> {
    clients: Mutex<HashMap<usize, Vec<Arc<R>>>>,
    rules: ArcSwap<Vec<Arc<R>>>,
}

impl<R> Default for Gate<R> {
    fn default() -> Self {
        Self {
            clients: Mutex::new(HashMap::new()),
            rules: ArcSwap::from_pointee(Vec::new()),
        }
    }
}

impl<R: GateRule> Gate<R> {
    fn update(&self, id: usize, rules: impl Iterator<Item = R>) {
        let rules: Vec<_> = rules.map(Arc::new).collect();
        let mut clients = self.clients.lock().unwrap();
        if rules.is_empty() {
            clients.remove(&id);
        } else {
            clients.insert(id, rules);
        }
        let mut merged: Vec<_> = clients.values().flatten().cloned().collect();
        merged.sort_unstable_by_key(|rule| !rule.is_unconditional());
        self.rules.store(Arc::new(merged));
    }

    fn remove(&self, id: usize) {
        self.update(id, std::iter::empty());
    }
}

#[derive(Debug)]
struct AccountRule {
    accounts: Option<HashSet<Pubkey>>,
    cuckoo: Option<Arc<CuckooFilter<[u8; 32]>>>,
    owners: Option<HashSet<Pubkey>>,
    txn_signature: Option<bool>,
}

impl From<&FilterAccountAggregate> for AccountRule {
    fn from(rule: &FilterAccountAggregate) -> Self {
        Self {
            accounts: rule.accounts.clone(),
            cuckoo: rule.accounts_cuckoo.clone(),
            owners: rule.owners.clone(),
            txn_signature: rule.txn_signature,
        }
    }
}

impl GateRule for AccountRule {
    fn is_unconditional(&self) -> bool {
        self.accounts.is_none()
            && self.cuckoo.is_none()
            && self.owners.is_none()
            && self.txn_signature.is_none()
    }
}

impl AccountRule {
    fn matches(&self, pubkey: &Pubkey, owner: &Pubkey, has_signature: bool) -> bool {
        self.txn_signature
            .is_none_or(|wanted| wanted == has_signature)
            && self
                .owners
                .as_ref()
                .is_none_or(|owners| owners.contains(owner))
            && ((self.accounts.is_none() && self.cuckoo.is_none())
                || self
                    .accounts
                    .as_ref()
                    .is_some_and(|keys| keys.contains(pubkey))
                || self
                    .cuckoo
                    .as_ref()
                    .is_some_and(|keys| keys.contains(&pubkey.to_bytes())))
    }
}

#[derive(Debug)]
struct TransactionRule {
    vote: Option<bool>,
    failed: Option<bool>,
    signature: Option<Signature>,
    include: HashSet<Pubkey>,
    exclude: HashSet<Pubkey>,
    required: Vec<Pubkey>,
    cuckoo: Option<Arc<CuckooFilter<[u8; 32]>>>,
}

impl From<&FilterTransactionsInner> for TransactionRule {
    fn from(rule: &FilterTransactionsInner) -> Self {
        // token_accounts owner expansion is intentionally outside the Processed fast-path contract.
        Self {
            vote: rule.vote,
            failed: rule.failed,
            signature: rule.signature,
            include: rule.account_include.set.clone(),
            exclude: rule.account_exclude.set.clone(),
            required: rule.account_required.clone(),
            cuckoo: rule.account_cuckoo.clone(),
        }
    }
}

impl GateRule for TransactionRule {
    fn is_unconditional(&self) -> bool {
        self.vote.is_none()
            && self.failed.is_none()
            && self.signature.is_none()
            && self.include.is_empty()
            && self.exclude.is_empty()
            && self.required.is_empty()
            && self.cuckoo.is_none()
    }
}

/// Borrow static and ALT keys from the callback; misses never allocate a temporary set.
#[derive(Clone, Copy)]
struct AccountKeys<'a> {
    static_keys: &'a [Pubkey],
    writable: &'a [Pubkey],
    readonly: &'a [Pubkey],
}

impl AccountKeys<'_> {
    fn iter(&self) -> impl Iterator<Item = &Pubkey> {
        self.static_keys
            .iter()
            .chain(self.writable)
            .chain(self.readonly)
    }

    fn contains(&self, key: &Pubkey) -> bool {
        self.static_keys.contains(key) || self.writable.contains(key) || self.readonly.contains(key)
    }

    fn overlaps(&self, keys: &HashSet<Pubkey>) -> bool {
        match keys.len() {
            0 => false,
            1 => self.contains(keys.iter().next().unwrap()),
            _ => self.iter().any(|key| keys.contains(key)),
        }
    }
}

impl TransactionRule {
    fn matches(
        &self,
        vote: bool,
        failed: bool,
        signature: &Signature,
        keys: AccountKeys<'_>,
    ) -> bool {
        self.vote.is_none_or(|wanted| wanted == vote)
            && self.failed.is_none_or(|wanted| wanted == failed)
            && self.signature.as_ref().is_none_or(|wanted| wanted == signature)
            && !keys.overlaps(&self.exclude)
            && ((self.include.is_empty() && self.cuckoo.is_none())
                || keys.overlaps(&self.include)
                || self.cuckoo.as_ref().is_some_and(|filter| keys.iter().any(|key| filter.contains(&key.to_bytes()))))
            && self.required.len() <= keys.static_keys.len() + keys.writable.len() + keys.readonly.len()
            // Count-based matching is incorrect when callback keys contain duplicates.
            && self.required.iter().all(|key| keys.contains(key))
    }
}

#[derive(Debug, Default, Clone)]
pub struct FastPathFilterGates {
    accounts: Arc<Gate<AccountRule>>,
    transactions: Arc<Gate<TransactionRule>>,
    deshred: Arc<Gate<TransactionRule>>,
}

impl FastPathFilterGates {
    pub fn update_client(&self, id: usize, filter: &Filter) {
        self.accounts.update(
            id,
            filter
                .accounts
                .strategy
                .aggregates()
                .iter()
                .map(AccountRule::from),
        );
        self.transactions.update(
            id,
            filter
                .transactions
                .filters
                .iter()
                .chain(&filter.transactions_status.filters)
                .map(|(_, rule)| TransactionRule::from(rule)),
        );
    }

    pub fn remove_client(&self, id: usize) {
        self.accounts.remove(id);
        self.transactions.remove(id);
    }

    pub fn update_deshred_client(&self, id: usize, filter: &DeshredFilter) {
        self.deshred.update(
            id,
            filter
                .deshred_transactions
                .filters
                .values()
                .map(|rule| TransactionRule {
                    vote: rule.vote,
                    failed: None,
                    signature: None,
                    include: rule.account_include.clone(),
                    exclude: rule.account_exclude.clone(),
                    required: rule.account_required.iter().copied().collect(),
                    cuckoo: None,
                }),
        );
    }

    pub fn remove_deshred_client(&self, id: usize) {
        self.deshred.remove(id);
    }

    pub fn allows_account(&self, pubkey: &Pubkey, owner: &Pubkey, has_signature: bool) -> bool {
        self.accounts
            .rules
            .load()
            .iter()
            .any(|rule| rule.matches(pubkey, owner, has_signature))
    }

    pub fn transaction_account_keys(
        &self,
        info: &ReplicaTransactionInfoV3<'_>,
    ) -> Option<HashSet<Pubkey>> {
        let loaded = &info.transaction_status_meta.loaded_addresses;
        let keys = AccountKeys {
            static_keys: info.transaction.message.static_account_keys(),
            writable: &loaded.writable,
            readonly: &loaded.readonly,
        };
        let allowed = self.transactions.rules.load().iter().any(|rule| {
            rule.matches(
                info.is_vote,
                info.transaction_status_meta.status.is_err(),
                info.signature,
                keys,
            )
        });
        // Release the snapshot before allocating or converting the accepted transaction.
        allowed.then(|| keys.iter().copied().collect())
    }

    pub fn allows_deshred(&self, info: &ReplicaDeshredTransactionInfoVersions<'_>) -> bool {
        let (vote, signature, transaction, loaded) = match info {
            ReplicaDeshredTransactionInfoVersions::V0_0_1(info) => (
                info.is_vote,
                info.signature,
                info.transaction,
                info.loaded_addresses,
            ),
            ReplicaDeshredTransactionInfoVersions::V0_0_2(info) => (
                info.is_vote,
                info.signature,
                info.transaction,
                info.loaded_addresses,
            ),
        };
        let keys = AccountKeys {
            static_keys: transaction.message.static_account_keys(),
            writable: loaded.map_or(&[], |loaded| loaded.writable.as_slice()),
            readonly: loaded.map_or(&[], |loaded| loaded.readonly.as_slice()),
        };
        self.deshred
            .rules
            .load()
            .iter()
            .any(|rule| rule.matches(vote, false, signature, keys))
    }
}

#[cfg(test)]
mod tests {
    use {
        super::*,
        crate::plugin::{
            filter::{fixtures, limits::FilterLimits},
            message::{Message, MessageDeshredTransaction, MessageTransaction},
        },
        agave_geyser_plugin_interface::geyser_plugin_interface::{
            ReplicaDeshredTransactionInfo, ReplicaDeshredTransactionInfoV2,
        },
        solana_hash::Hash,
        solana_message::{legacy::Message as LegacyMessage, v0::LoadedAddresses, VersionedMessage},
        solana_transaction::versioned::VersionedTransaction,
        solana_transaction_error::TransactionError,
        solana_transaction_status::TransactionStatusMeta,
        yellowstone_grpc_proto::geyser::{
            SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterAccounts,
            SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterTransactions,
        },
    };

    fn filter(request: SubscribeRequest) -> Filter {
        Filter::new(
            &request,
            &FilterLimits::default(),
            &mut fixtures::filter_names(),
        )
        .unwrap()
    }

    fn account_filter(rule: SubscribeRequestFilterAccounts) -> Filter {
        filter(SubscribeRequest {
            accounts: HashMap::from([("account".into(), rule)]),
            ..Default::default()
        })
    }

    fn versioned(keys: Vec<Pubkey>) -> VersionedTransaction {
        VersionedTransaction {
            signatures: vec![Signature::default()],
            message: VersionedMessage::Legacy(LegacyMessage {
                account_keys: keys,
                ..Default::default()
            }),
        }
    }

    #[test]
    fn account_gate_preserves_coarse_filter_semantics() {
        let keys = fixtures::deterministic_pubkeys(61, 4);
        let mut cuckoo = CuckooFilter::with_capacity(8).unwrap();
        cuckoo.insert(&keys[1].to_bytes()).unwrap();
        let gates = FastPathFilterGates::default();
        assert!(!gates.allows_account(&keys[0], &keys[2], true));
        for rule in [
            SubscribeRequestFilterAccounts::default(),
            SubscribeRequestFilterAccounts {
                account: vec![keys[0].to_string()],
                ..Default::default()
            },
            SubscribeRequestFilterAccounts {
                owner: vec![keys[2].to_string()],
                nonempty_txn_signature: Some(true),
                ..Default::default()
            },
            SubscribeRequestFilterAccounts {
                account: vec![keys[0].to_string()],
                cuckoo_accounts_filter: Some((&cuckoo).into()),
                owner: vec![keys[2].to_string()],
                nonempty_txn_signature: Some(false),
                ..Default::default()
            },
        ] {
            let filter = account_filter(rule);
            gates.update_client(1, &filter);
            for pubkey in &keys {
                for owner in &keys {
                    for signature in [None, Some(Signature::default())] {
                        let message = Message::Account(fixtures::message_account(
                            fixtures::account_info(*pubkey, *owner, vec![], 1, signature),
                        ));
                        assert_eq!(
                            gates.allows_account(pubkey, owner, signature.is_some()),
                            !filter.get_updates(&message, None).is_empty()
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn client_union_replacement_removal_and_rule_sharing() {
        let keys = fixtures::deterministic_pubkeys(62, 3);
        let gates = FastPathFilterGates::default();
        let make_filter = |key: Pubkey| {
            account_filter(SubscribeRequestFilterAccounts {
                account: vec![key.to_string()],
                ..Default::default()
            })
        };
        gates.update_client(1, &make_filter(keys[0]));
        let first = Arc::clone(&gates.accounts.rules.load_full()[0]);
        gates.update_client(2, &make_filter(keys[1]));
        assert!(gates
            .accounts
            .rules
            .load()
            .iter()
            .any(|rule| Arc::ptr_eq(rule, &first)));
        assert!(gates.allows_account(&keys[0], &keys[2], false));
        assert!(gates.allows_account(&keys[1], &keys[2], false));
        gates.update_client(1, &make_filter(keys[2]));
        assert!(!gates.allows_account(&keys[0], &keys[2], false));
        gates.remove_client(2);
        assert!(!gates.allows_account(&keys[1], &keys[2], false));
        gates.update_client(1, &Filter::default());
        assert!(!gates.allows_account(&keys[2], &keys[2], false));
    }

    #[test]
    fn account_data_predicates_stay_in_client_filter() {
        use yellowstone_grpc_proto::geyser::{
            subscribe_request_filter_accounts_filter::Filter as Predicate,
            SubscribeRequestFilterAccountsFilter,
        };
        let key = Pubkey::new_unique();
        let filter = account_filter(SubscribeRequestFilterAccounts {
            filters: vec![SubscribeRequestFilterAccountsFilter {
                filter: Some(Predicate::Datasize(100)),
            }],
            ..Default::default()
        });
        let gates = FastPathFilterGates::default();
        gates.update_client(1, &filter);
        assert!(gates.allows_account(&key, &key, false));
        let message = Message::Account(fixtures::simple_message_account(key, key));
        assert!(filter.get_updates(&message, None).is_empty());
    }

    #[test]
    fn transaction_gate_matches_client_filter_with_alt_and_status_only() {
        let keys = fixtures::deterministic_pubkeys(63, 5);
        let transaction = versioned(vec![keys[0], keys[0]]);
        let signature = Signature::default();
        let hash = Hash::default();
        let mut cuckoo = CuckooFilter::with_capacity(8).unwrap();
        cuckoo.insert(&keys[2].to_bytes()).unwrap();
        let gates = FastPathFilterGates::default();
        let rules = [
            SubscribeRequestFilterTransactions::default(),
            SubscribeRequestFilterTransactions {
                vote: Some(false),
                failed: Some(false),
                ..Default::default()
            },
            SubscribeRequestFilterTransactions {
                signature: Some(signature.to_string()),
                ..Default::default()
            },
            SubscribeRequestFilterTransactions {
                signature: Some(Signature::from([7; 64]).to_string()),
                ..Default::default()
            },
            SubscribeRequestFilterTransactions {
                account_include: vec![keys[1].to_string()],
                account_required: vec![keys[2].to_string()],
                ..Default::default()
            },
            SubscribeRequestFilterTransactions {
                account_include: vec![keys[3].to_string()],
                cuckoo_account_include: Some((&cuckoo).into()),
                ..Default::default()
            },
            SubscribeRequestFilterTransactions {
                account_include: vec![keys[0].to_string()],
                cuckoo_account_include: Some((&cuckoo).into()),
                ..Default::default()
            },
            SubscribeRequestFilterTransactions {
                account_exclude: vec![keys[2].to_string()],
                ..Default::default()
            },
            SubscribeRequestFilterTransactions {
                account_include: vec![keys[1].to_string(), keys[4].to_string()],
                account_exclude: vec![keys[3].to_string(), keys[4].to_string()],
                ..Default::default()
            },
            // Duplicated callback keys must not stand in for a missing required key.
            SubscribeRequestFilterTransactions {
                account_required: vec![keys[0].to_string(), keys[3].to_string()],
                ..Default::default()
            },
            SubscribeRequestFilterTransactions {
                account_required: keys.iter().map(ToString::to_string).collect(),
                ..Default::default()
            },
        ];
        for status_only in [false, true] {
            for rule in &rules {
                let rules = HashMap::from([("tx".into(), rule.clone())]);
                let filter = filter(if status_only {
                    SubscribeRequest {
                        transactions_status: rules,
                        ..Default::default()
                    }
                } else {
                    SubscribeRequest {
                        transactions: rules,
                        ..Default::default()
                    }
                });
                gates.update_client(1, &filter);
                for is_vote in [false, true] {
                    for failed in [false, true] {
                        for include_alt in [false, true] {
                            let meta = TransactionStatusMeta {
                                status: if failed {
                                    Err(TransactionError::AccountNotFound)
                                } else {
                                    Ok(())
                                },
                                loaded_addresses: if include_alt {
                                    LoadedAddresses {
                                        writable: vec![keys[1]],
                                        readonly: vec![keys[2]],
                                    }
                                } else {
                                    LoadedAddresses::default()
                                },
                                ..Default::default()
                            };
                            let info = ReplicaTransactionInfoV3 {
                                signature: &signature,
                                message_hash: &hash,
                                is_vote,
                                transaction: &transaction,
                                transaction_status_meta: &meta,
                                index: 2,
                            };
                            let original = MessageTransaction::from_geyser(&info, 10, 99);
                            let accepted = gates.transaction_account_keys(&info);
                            assert_eq!(
                                accepted.is_some(),
                                !filter
                                    .get_updates(
                                        &Message::Transaction(Arc::new(original.clone())),
                                        None
                                    )
                                    .is_empty(),
                                "rule={rule:?}, status_only={status_only}, alt={include_alt}"
                            );
                            if let Some(keys) = accepted {
                                let rebuilt = MessageTransaction::from_geyser_with_account_keys(
                                    &info, 10, 99, keys,
                                );
                                assert_eq!(original.transaction, rebuilt.transaction);
                                assert_eq!(rebuilt.bank_id, 99);
                                assert!(rebuilt.transaction.pre_encoded.get().is_none());
                            }
                        }
                    }
                }
            }
        }
        gates.remove_client(1);
        assert!(gates.transactions.rules.load().is_empty());
    }

    #[test]
    fn deshred_versions_preserve_alt_matching_and_shred_metadata() {
        let keys = fixtures::deterministic_pubkeys(64, 4);
        let transaction = versioned(vec![keys[0], keys[0]]);
        let signature = Signature::default();
        let loaded = LoadedAddresses {
            writable: vec![keys[1]],
            readonly: vec![keys[2]],
        };
        let gates = FastPathFilterGates::default();
        for rule in [
            SubscribeRequestFilterDeshredTransactions {
                vote: Some(false),
                account_include: vec![keys[1].to_string()],
                account_required: vec![keys[2].to_string()],
                ..Default::default()
            },
            SubscribeRequestFilterDeshredTransactions {
                account_required: vec![keys[0].to_string(), keys[3].to_string()],
                ..Default::default()
            },
            SubscribeRequestFilterDeshredTransactions {
                account_exclude: vec![keys[2].to_string()],
                ..Default::default()
            },
        ] {
            let filter = DeshredFilter::new(
                &SubscribeDeshredRequest {
                    deshred_transactions: HashMap::from([("deshred".into(), rule)]),
                    ..Default::default()
                },
                &FilterLimits::default(),
                &mut fixtures::filter_names(),
            )
            .unwrap();
            gates.update_deshred_client(1, &filter);
            for loaded in [None, Some(&loaded)] {
                for is_vote in [false, true] {
                    let v1 = ReplicaDeshredTransactionInfo {
                        signature: &signature,
                        is_vote,
                        transaction: &transaction,
                        loaded_addresses: loaded,
                    };
                    let v2 = ReplicaDeshredTransactionInfoV2 {
                        signature: &signature,
                        is_vote,
                        transaction: &transaction,
                        loaded_addresses: loaded,
                        completed_data_set_starting_shred_index: 10,
                        completed_data_set_ending_shred_index_exclusive: 20,
                    };
                    for info in [
                        ReplicaDeshredTransactionInfoVersions::V0_0_1(&v1),
                        ReplicaDeshredTransactionInfoVersions::V0_0_2(&v2),
                    ] {
                        let is_v2 =
                            matches!(info, ReplicaDeshredTransactionInfoVersions::V0_0_2(_));
                        let allowed = gates.allows_deshred(&info);
                        let message = MessageDeshredTransaction::from_geyser_versioned(info, 10);
                        assert_eq!(
                            message.transaction.completed_data_set_starting_shred_index,
                            if is_v2 { 10 } else { 0 }
                        );
                        assert_eq!(
                            message
                                .transaction
                                .completed_data_set_ending_shred_index_exclusive,
                            if is_v2 { 20 } else { 0 }
                        );
                        assert_eq!(
                            allowed,
                            !filter
                                .get_updates(&Message::DeshredTransaction(Arc::new(message)), None)
                                .is_empty()
                        );
                    }
                }
            }
        }
        gates.remove_deshred_client(1);
        assert!(gates.deshred.rules.load().is_empty());
    }
}
