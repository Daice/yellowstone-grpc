use {
    crate::plugin::{
        filter::{
            limits::{
                FilterLimits, FilterLimitsAccounts, FilterLimitsBlocks, FilterLimitsBlocksMeta,
                FilterLimitsCheckError, FilterLimitsDeshredTransactions, FilterLimitsEntries,
                FilterLimitsSlots, FilterLimitsTransactions,
            },
            message::{
                FilteredUpdate, FilteredUpdateBlock, FilteredUpdateDeshred,
                FilteredUpdateDeshredOneof, FilteredUpdateFilters, FilteredUpdateOneof,
                FilteredUpdates, FilteredUpdatesDeshred,
            },
            name::{FilterName, FilterNameError, FilterNames},
        },
        message::{
            CommitmentLevel, Message, MessageAccount, MessageBlock, MessageBlockMeta,
            MessageDeshredTransaction, MessageEntry, MessageSlot, MessageTransaction, SlotStatus,
        },
    },
    agave_geyser_plugin_interface::geyser_plugin_interface::{
        ReplicaAccountInfoV3, ReplicaDeshredTransactionInfo, ReplicaTransactionInfoV3,
    },
    arc_swap::ArcSwap,
    base64::{engine::general_purpose::STANDARD as base64_engine, Engine},
    bytes::buf::BufMut,
    prost::encoding::{encode_key, encode_varint, WireType},
    solana_pubkey::{ParsePubkeyError, Pubkey},
    solana_signature::{ParseSignatureError, Signature},
    spl_token_2022_interface::{
        generic_token_account::GenericTokenAccount, state::Account as TokenAccount,
    },
    std::{
        collections::{HashMap, HashSet},
        ops::Range,
        str::FromStr,
        sync::{Arc, RwLock},
    },
    yellowstone_grpc_proto::geyser::{
        subscribe_request_filter_accounts_filter::Filter as AccountsFilterDataOneof,
        subscribe_request_filter_accounts_filter_lamports::Cmp as AccountsFilterLamports,
        subscribe_request_filter_accounts_filter_memcmp::Data as AccountsFilterMemcmpOneof,
        CommitmentLevel as CommitmentLevelProto, SubscribeDeshredRequest, SubscribeRequest,
        SubscribeRequestAccountsDataSlice, SubscribeRequestFilterAccounts,
        SubscribeRequestFilterAccountsFilter, SubscribeRequestFilterAccountsFilterLamports,
        SubscribeRequestFilterBlocks, SubscribeRequestFilterBlocksMeta,
        SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterEntry,
        SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
    },
};

#[derive(Debug, thiserror::Error)]
pub enum FilterError {
    #[error(transparent)]
    Name(#[from] FilterNameError),
    #[error(transparent)]
    LimitsCheck(#[from] FilterLimitsCheckError),

    #[error("failed to create CommitmentLevel from {commitment}")]
    InvalidCommitment { commitment: i32 },
    #[error(transparent)]
    InvalidPubkey(#[from] ParsePubkeyError),
    #[error(transparent)]
    InvalidSignature(#[from] ParseSignatureError),

    #[error("Too many filters provided; max {max}")]
    CreateAccountStateMaxFilters { max: usize },
    #[error("{0}")]
    CreateAccountState(&'static str),
    #[error("`include_{0}` is not allowed")]
    CreateBlocksNotAllowed(&'static str),
    #[error("failed to create filter: data slices out of order")]
    CreateDataSliceOutOfOrder,
    #[error("failed to create filter: data slices overlapped")]
    CreateDataSliceOverlap,
}

pub type FilterResult<T> = Result<T, FilterError>;

macro_rules! filtered_updates_once_owned {
    ($filters:ident, $message:expr, $created_at:expr) => {{
        let mut messages = FilteredUpdates::new();
        if !$filters.is_empty() {
            messages.push(FilteredUpdate::new($filters, $message, $created_at));
        }
        messages
    }};
}

macro_rules! filtered_updates_once_ref {
    ($filters:ident, $message:expr, $created_at:expr) => {{
        let mut messages = FilteredUpdates::new();
        if !$filters.is_empty() {
            let mut message_filters = FilteredUpdateFilters::new();
            for filter in $filters {
                message_filters.push(filter.clone());
            }
            messages.push(FilteredUpdate::new(message_filters, $message, $created_at));
        }
        messages
    }};
}

#[derive(Debug, Clone)]
pub struct Filter {
    accounts: FilterAccounts,
    slots: FilterSlots,
    transactions: FilterTransactions,
    transactions_status: FilterTransactions,
    entries: FilterEntries,
    blocks: FilterBlocks,
    blocks_meta: FilterBlocksMeta,
    commitment: CommitmentLevel,
    accounts_data_slice: FilterAccountsDataSlice,
    ping: Option<i32>,
}

#[derive(Debug, Clone, Default)]
pub struct AccountFilterRule {
    nonempty_txn_signature: Option<bool>,
    account: HashSet<Pubkey>,
    account_required: bool,
    owner: HashSet<Pubkey>,
    owner_required: bool,
}

impl AccountFilterRule {
    fn matches_prepared(&self, has_txn_signature: bool, pubkey: &Pubkey, owner: &Pubkey) -> bool {
        if let Some(nonempty_txn_signature) = self.nonempty_txn_signature {
            if nonempty_txn_signature != has_txn_signature {
                return false;
            }
        }

        if self.account_required && !self.account.contains(pubkey) {
            return false;
        }

        if self.owner_required && !self.owner.contains(owner) {
            return false;
        }

        true
    }
}

#[derive(Debug)]
struct AccountFilterGateInner {
    client_rules: HashMap<usize, Arc<[AccountFilterRule]>>,
}

impl Default for AccountFilterGateInner {
    fn default() -> Self {
        Self {
            client_rules: HashMap::new(),
        }
    }
}

impl AccountFilterGateInner {
    fn build_merged(&self) -> Arc<Vec<AccountFilterRule>> {
        let total_rules = self
            .client_rules
            .values()
            .map(|rules| rules.len())
            .sum::<usize>();
        let mut merged = Vec::with_capacity(total_rules);
        for rules in self.client_rules.values() {
            merged.extend(rules.iter().cloned());
        }
        Arc::new(merged)
    }
}

#[derive(Debug, Clone)]
pub struct AccountFilterGate {
    inner: Arc<RwLock<AccountFilterGateInner>>,
    merged_rules: Arc<ArcSwap<Vec<AccountFilterRule>>>,
}

impl Default for AccountFilterGate {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(AccountFilterGateInner::default())),
            merged_rules: Arc::new(ArcSwap::from_pointee(Vec::new())),
        }
    }
}

impl AccountFilterGate {
    pub fn update_client_rules(&self, client_id: usize, rules: Vec<AccountFilterRule>) {
        let merged = {
            let mut inner = match self.inner.write() {
                Ok(inner) => inner,
                Err(poisoned) => poisoned.into_inner(),
            };

            if rules.is_empty() {
                inner.client_rules.remove(&client_id);
            } else {
                inner.client_rules.insert(client_id, rules.into());
            }
            inner.build_merged()
        };
        self.merged_rules.store(merged);
    }

    pub fn remove_client(&self, client_id: usize) {
        let merged = {
            let mut inner = match self.inner.write() {
                Ok(inner) => inner,
                Err(poisoned) => poisoned.into_inner(),
            };

            if inner.client_rules.remove(&client_id).is_some() {
                inner.build_merged()
            } else {
                return;
            }
        };
        self.merged_rules.store(merged);
    }

    pub fn allows(&self, account: &ReplicaAccountInfoV3<'_>) -> bool {
        let Ok(pubkey) = Pubkey::try_from(account.pubkey) else {
            return false;
        };
        let Ok(owner) = Pubkey::try_from(account.owner) else {
            return false;
        };
        self.allows_prepared(account.txn.is_some(), &pubkey, &owner)
    }

    pub fn allows_prepared(
        &self,
        has_txn_signature: bool,
        pubkey: &Pubkey,
        owner: &Pubkey,
    ) -> bool {
        let rules = self.merged_rules.load_full();

        if rules.is_empty() {
            return false;
        }

        rules
            .iter()
            .any(|rule| rule.matches_prepared(has_txn_signature, pubkey, owner))
    }
}

#[derive(Debug, Clone, Default)]
pub struct TransactionFilterRule {
    pub vote: Option<bool>,
    pub failed: Option<bool>,
    pub signature: Option<Signature>,
    pub account_include: Vec<Pubkey>,
    pub account_exclude: Vec<Pubkey>,
    pub account_required: Vec<Pubkey>,
    account_include_set: HashSet<Pubkey>,
    account_exclude_set: HashSet<Pubkey>,
    account_required_set: HashSet<Pubkey>,
}

impl TransactionFilterRule {
    fn from_inner(inner: &FilterTransactionsInner) -> Self {
        let account_include_set = inner.account_include.clone();
        let account_exclude_set = inner.account_exclude.clone();
        let account_required_set = inner.account_required.clone();
        Self {
            vote: inner.vote,
            failed: inner.failed,
            signature: inner.signature.clone(),
            account_include: account_include_set.iter().copied().collect(),
            account_exclude: account_exclude_set.iter().copied().collect(),
            account_required: account_required_set.iter().copied().collect(),
            account_include_set,
            account_exclude_set,
            account_required_set,
        }
    }

    fn matches_prepared(
        &self,
        tx_is_vote: bool,
        tx_is_failed: bool,
        signature: &Signature,
        account_keys: &TxAccountKeysView<'_>,
    ) -> bool {
        if let Some(expected_vote) = self.vote {
            if expected_vote != tx_is_vote {
                return false;
            }
        }

        if let Some(expected_failed) = self.failed {
            if expected_failed != tx_is_failed {
                return false;
            }
        }

        if let Some(expected_signature) = &self.signature {
            if expected_signature != signature {
                return false;
            }
        }

        if !self.account_include_set.is_empty()
            && !account_keys.any_in_set(&self.account_include_set)
        {
            return false;
        }

        if !self.account_exclude_set.is_empty()
            && account_keys.any_in_set(&self.account_exclude_set)
        {
            return false;
        }

        if !self.account_required_set.is_empty()
            && !self
                .account_required_set
                .iter()
                .all(|required| account_keys.contains(required))
        {
            return false;
        }

        true
    }
}

#[derive(Debug, Clone, Copy)]
struct TxAccountKeysView<'a> {
    static_keys: &'a [Pubkey],
    loaded_writable: &'a [Pubkey],
    loaded_readonly: &'a [Pubkey],
}

impl<'a> TxAccountKeysView<'a> {
    fn new(tx_info: &'a ReplicaTransactionInfoV3<'_>) -> Self {
        Self {
            static_keys: tx_info.transaction.message.static_account_keys(),
            loaded_writable: &tx_info.transaction_status_meta.loaded_addresses.writable,
            loaded_readonly: &tx_info.transaction_status_meta.loaded_addresses.readonly,
        }
    }

    #[inline]
    fn any_in_set(&self, set: &HashSet<Pubkey>) -> bool {
        self.static_keys.iter().any(|key| set.contains(key))
            || self.loaded_writable.iter().any(|key| set.contains(key))
            || self.loaded_readonly.iter().any(|key| set.contains(key))
    }

    #[inline]
    fn contains(&self, key: &Pubkey) -> bool {
        self.static_keys.contains(key)
            || self.loaded_writable.contains(key)
            || self.loaded_readonly.contains(key)
    }

    fn to_hash_set(&self) -> HashSet<Pubkey> {
        let mut account_keys = HashSet::with_capacity(
            self.static_keys.len() + self.loaded_writable.len() + self.loaded_readonly.len(),
        );
        account_keys.extend(self.static_keys.iter().copied());
        account_keys.extend(self.loaded_writable.iter().copied());
        account_keys.extend(self.loaded_readonly.iter().copied());
        account_keys
    }
}

#[derive(Debug)]
struct TransactionFilterGateInner {
    client_rules: HashMap<usize, Arc<[TransactionFilterRule]>>,
}

impl Default for TransactionFilterGateInner {
    fn default() -> Self {
        Self {
            client_rules: HashMap::new(),
        }
    }
}

impl TransactionFilterGateInner {
    fn build_merged(&self) -> Arc<Vec<TransactionFilterRule>> {
        let total_rules = self
            .client_rules
            .values()
            .map(|rules| rules.len())
            .sum::<usize>();
        let mut merged = Vec::with_capacity(total_rules);
        for rules in self.client_rules.values() {
            merged.extend(rules.iter().cloned());
        }
        Arc::new(merged)
    }
}

#[derive(Debug, Clone)]
pub struct TransactionFilterGate {
    inner: Arc<RwLock<TransactionFilterGateInner>>,
    merged_rules: Arc<ArcSwap<Vec<TransactionFilterRule>>>,
}

impl Default for TransactionFilterGate {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(TransactionFilterGateInner::default())),
            merged_rules: Arc::new(ArcSwap::from_pointee(Vec::new())),
        }
    }
}

impl TransactionFilterGate {
    pub fn update_client_rules(&self, client_id: usize, rules: Vec<TransactionFilterRule>) {
        let merged = {
            let mut inner = match self.inner.write() {
                Ok(inner) => inner,
                Err(poisoned) => poisoned.into_inner(),
            };

            if rules.is_empty() {
                inner.client_rules.remove(&client_id);
            } else {
                inner.client_rules.insert(client_id, rules.into());
            }
            inner.build_merged()
        };
        self.merged_rules.store(merged);
    }

    pub fn remove_client(&self, client_id: usize) {
        let merged = {
            let mut inner = match self.inner.write() {
                Ok(inner) => inner,
                Err(poisoned) => poisoned.into_inner(),
            };

            if inner.client_rules.remove(&client_id).is_some() {
                inner.build_merged()
            } else {
                return;
            }
        };
        self.merged_rules.store(merged);
    }

    pub fn allows(&self, tx_info: &ReplicaTransactionInfoV3<'_>) -> bool {
        self.allows_and_account_keys(tx_info).is_some()
    }

    pub fn allows_and_account_keys(
        &self,
        tx_info: &ReplicaTransactionInfoV3<'_>,
    ) -> Option<HashSet<Pubkey>> {
        let rules = self.merged_rules.load_full();

        if rules.is_empty() {
            return None;
        }

        let account_keys = TxAccountKeysView::new(tx_info);

        let is_vote = tx_info.is_vote;
        let is_failed = tx_info.transaction_status_meta.status.is_err();
        let signature = tx_info.signature;

        if rules
            .iter()
            .any(|rule| rule.matches_prepared(is_vote, is_failed, signature, &account_keys))
        {
            Some(account_keys.to_hash_set())
        } else {
            None
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct DeshredTransactionFilterRule {
    pub vote: Option<bool>,
    pub account_include: Vec<Pubkey>,
    pub account_exclude: Vec<Pubkey>,
    pub account_required: Vec<Pubkey>,
    account_include_set: HashSet<Pubkey>,
    account_exclude_set: HashSet<Pubkey>,
    account_required_set: HashSet<Pubkey>,
}

impl DeshredTransactionFilterRule {
    fn from_inner(inner: &FilterDeshredTransactionsInner) -> Self {
        let account_include_set = inner.account_include.clone();
        let account_exclude_set = inner.account_exclude.clone();
        let account_required_set = inner.account_required.clone();
        Self {
            vote: inner.vote,
            account_include: account_include_set.iter().copied().collect(),
            account_exclude: account_exclude_set.iter().copied().collect(),
            account_required: account_required_set.iter().copied().collect(),
            account_include_set,
            account_exclude_set,
            account_required_set,
        }
    }

    fn matches_prepared(
        &self,
        tx_is_vote: bool,
        account_keys: &DeshredTxAccountKeysView<'_>,
    ) -> bool {
        if let Some(expected_vote) = self.vote {
            if expected_vote != tx_is_vote {
                return false;
            }
        }

        if !self.account_include_set.is_empty()
            && !account_keys.any_in_set(&self.account_include_set)
        {
            return false;
        }

        if !self.account_exclude_set.is_empty()
            && account_keys.any_in_set(&self.account_exclude_set)
        {
            return false;
        }

        if !self.account_required_set.is_empty()
            && !self
                .account_required_set
                .iter()
                .all(|required| account_keys.contains(required))
        {
            return false;
        }

        true
    }
}

#[derive(Debug, Clone, Copy)]
struct DeshredTxAccountKeysView<'a> {
    static_keys: &'a [Pubkey],
    loaded_writable: &'a [Pubkey],
    loaded_readonly: &'a [Pubkey],
}

impl<'a> DeshredTxAccountKeysView<'a> {
    fn new(tx_info: &'a ReplicaDeshredTransactionInfo<'_>) -> Self {
        let (loaded_writable, loaded_readonly) = tx_info
            .loaded_addresses
            .map(|loaded_addresses| {
                (
                    loaded_addresses.writable.as_slice(),
                    loaded_addresses.readonly.as_slice(),
                )
            })
            .unwrap_or((&[][..], &[][..]));
        Self {
            static_keys: tx_info.transaction.message.static_account_keys(),
            loaded_writable,
            loaded_readonly,
        }
    }

    #[inline]
    fn any_in_set(&self, set: &HashSet<Pubkey>) -> bool {
        self.static_keys.iter().any(|key| set.contains(key))
            || self.loaded_writable.iter().any(|key| set.contains(key))
            || self.loaded_readonly.iter().any(|key| set.contains(key))
    }

    #[inline]
    fn contains(&self, key: &Pubkey) -> bool {
        self.static_keys.contains(key)
            || self.loaded_writable.contains(key)
            || self.loaded_readonly.contains(key)
    }
}

#[derive(Debug)]
struct DeshredTransactionFilterGateInner {
    client_rules: HashMap<usize, Arc<[DeshredTransactionFilterRule]>>,
}

impl Default for DeshredTransactionFilterGateInner {
    fn default() -> Self {
        Self {
            client_rules: HashMap::new(),
        }
    }
}

impl DeshredTransactionFilterGateInner {
    fn build_merged(&self) -> Arc<Vec<DeshredTransactionFilterRule>> {
        let total_rules = self
            .client_rules
            .values()
            .map(|rules| rules.len())
            .sum::<usize>();
        let mut merged = Vec::with_capacity(total_rules);
        for rules in self.client_rules.values() {
            merged.extend(rules.iter().cloned());
        }
        Arc::new(merged)
    }
}

#[derive(Debug, Clone)]
pub struct DeshredTransactionFilterGate {
    inner: Arc<RwLock<DeshredTransactionFilterGateInner>>,
    merged_rules: Arc<ArcSwap<Vec<DeshredTransactionFilterRule>>>,
}

impl Default for DeshredTransactionFilterGate {
    fn default() -> Self {
        Self {
            inner: Arc::new(RwLock::new(DeshredTransactionFilterGateInner::default())),
            merged_rules: Arc::new(ArcSwap::from_pointee(Vec::new())),
        }
    }
}

impl DeshredTransactionFilterGate {
    pub fn update_client_rules(&self, client_id: usize, rules: Vec<DeshredTransactionFilterRule>) {
        let merged = {
            let mut inner = match self.inner.write() {
                Ok(inner) => inner,
                Err(poisoned) => poisoned.into_inner(),
            };

            if rules.is_empty() {
                inner.client_rules.remove(&client_id);
            } else {
                inner.client_rules.insert(client_id, rules.into());
            }
            inner.build_merged()
        };
        self.merged_rules.store(merged);
    }

    pub fn remove_client(&self, client_id: usize) {
        let merged = {
            let mut inner = match self.inner.write() {
                Ok(inner) => inner,
                Err(poisoned) => poisoned.into_inner(),
            };

            if inner.client_rules.remove(&client_id).is_some() {
                inner.build_merged()
            } else {
                return;
            }
        };
        self.merged_rules.store(merged);
    }

    pub fn allows(&self, tx_info: &ReplicaDeshredTransactionInfo<'_>) -> bool {
        let rules = self.merged_rules.load_full();

        if rules.is_empty() {
            return false;
        }

        let account_keys = DeshredTxAccountKeysView::new(tx_info);
        rules
            .iter()
            .any(|rule| rule.matches_prepared(tx_info.is_vote, &account_keys))
    }

    #[cfg(test)]
    fn allows_message(&self, message: &MessageDeshredTransaction) -> bool {
        let rules = self.merged_rules.load_full();

        if rules.is_empty() {
            return false;
        }

        rules.iter().any(|rule| {
            if let Some(expected_vote) = rule.vote {
                if expected_vote != message.transaction.is_vote {
                    return false;
                }
            }

            if !rule.account_include_set.is_empty()
                && !message
                    .transaction
                    .all_account_keys()
                    .any(|key| rule.account_include_set.contains(key))
            {
                return false;
            }

            if !rule.account_exclude_set.is_empty()
                && message
                    .transaction
                    .all_account_keys()
                    .any(|key| rule.account_exclude_set.contains(key))
            {
                return false;
            }

            if !rule.account_required_set.is_empty() {
                let all_keys: HashSet<&Pubkey> = message.transaction.all_account_keys().collect();
                if !rule
                    .account_required_set
                    .iter()
                    .all(|key| all_keys.contains(key))
                {
                    return false;
                }
            }

            true
        })
    }
}

impl Default for Filter {
    fn default() -> Self {
        Self {
            accounts: FilterAccounts::default(),
            slots: FilterSlots::default(),
            transactions: FilterTransactions {
                filter_type: FilterTransactionsType::Transaction,
                filters: HashMap::new(),
            },
            transactions_status: FilterTransactions {
                filter_type: FilterTransactionsType::TransactionStatus,
                filters: HashMap::new(),
            },
            entries: FilterEntries::default(),
            blocks: FilterBlocks::default(),
            blocks_meta: FilterBlocksMeta::default(),
            commitment: CommitmentLevel::Processed,
            accounts_data_slice: FilterAccountsDataSlice::default(),
            ping: None,
        }
    }
}

impl Filter {
    pub fn new(
        config: &SubscribeRequest,
        limits: &FilterLimits,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        Ok(Self {
            accounts: FilterAccounts::new(&config.accounts, &limits.accounts, names)?,
            slots: FilterSlots::new(&config.slots, &limits.slots, names)?,
            transactions: FilterTransactions::new(
                &config.transactions,
                &limits.transactions,
                FilterTransactionsType::Transaction,
                names,
            )?,
            transactions_status: FilterTransactions::new(
                &config.transactions_status,
                &limits.transactions_status,
                FilterTransactionsType::TransactionStatus,
                names,
            )?,
            entries: FilterEntries::new(&config.entry, &limits.entries, names)?,
            blocks: FilterBlocks::new(&config.blocks, &limits.blocks, names)?,
            blocks_meta: FilterBlocksMeta::new(&config.blocks_meta, &limits.blocks_meta, names)?,
            commitment: Self::decode_commitment(config.commitment)?,
            accounts_data_slice: FilterAccountsDataSlice::new(
                &config.accounts_data_slice,
                limits.accounts.data_slice_max,
            )?,
            ping: config.ping.as_ref().map(|msg| msg.id),
        })
    }

    fn decode_commitment(commitment: Option<i32>) -> FilterResult<CommitmentLevel> {
        let commitment = commitment.unwrap_or(CommitmentLevelProto::Processed as i32);
        let commitment = CommitmentLevelProto::try_from(commitment)
            .map(Into::into)
            .map_err(|_error| FilterError::InvalidCommitment { commitment })?;
        if !matches!(
            commitment,
            CommitmentLevel::Processed | CommitmentLevel::Confirmed | CommitmentLevel::Finalized
        ) {
            Err(FilterError::InvalidCommitment {
                commitment: commitment as i32,
            })
        } else {
            Ok(commitment)
        }
    }

    fn decode_pubkeys<'a>(
        pubkeys: &'a [String],
        limit: &'a HashSet<Pubkey>,
    ) -> impl Iterator<Item = FilterResult<Pubkey>> + 'a {
        pubkeys.iter().map(|value| {
            let pubkey = Pubkey::from_str(value)?;
            FilterLimits::check_pubkey_reject(&pubkey, limit)?;
            Ok(pubkey)
        })
    }

    fn decode_pubkeys_into_set(
        pubkeys: &[String],
        limit: &HashSet<Pubkey>,
    ) -> FilterResult<HashSet<Pubkey>> {
        Self::decode_pubkeys(pubkeys, limit).collect::<FilterResult<_>>()
    }

    pub fn get_metrics(&self) -> [(&'static str, usize); 8] {
        [
            ("accounts", self.accounts.filters.len()),
            ("slots", self.slots.filters.len()),
            ("transactions", self.transactions.filters.len()),
            (
                "transactions_status",
                self.transactions_status.filters.len(),
            ),
            ("entries", self.entries.filters.len()),
            ("blocks", self.blocks.filters.len()),
            ("blocks_meta", self.blocks_meta.filters.len()),
            (
                "all",
                self.accounts.filters.len()
                    + self.slots.filters.len()
                    + self.transactions.filters.len()
                    + self.transactions_status.filters.len()
                    + self.entries.filters.len()
                    + self.blocks.filters.len()
                    + self.blocks_meta.filters.len(),
            ),
        ]
    }

    pub fn has_blocks_subscriptions(&self) -> bool {
        !self.blocks.filters.is_empty()
    }

    pub fn has_slots_subscriptions(&self) -> bool {
        !self.slots.filters.is_empty()
    }

    pub fn get_account_filter_rules(&self) -> Vec<AccountFilterRule> {
        let mut rules = Vec::with_capacity(self.accounts.filters.len());
        self.accounts.collect_rules(&mut rules);
        rules
    }

    pub fn get_transaction_filter_rules(&self) -> Vec<TransactionFilterRule> {
        let mut rules = Vec::with_capacity(
            self.transactions.filters.len() + self.transactions_status.filters.len(),
        );
        self.transactions.collect_rules(&mut rules);
        self.transactions_status.collect_rules(&mut rules);
        rules
    }

    pub const fn get_commitment_level(&self) -> CommitmentLevel {
        self.commitment
    }

    pub fn get_updates(
        &self,
        message: &Message,
        commitment: Option<CommitmentLevel>,
    ) -> FilteredUpdates {
        match message {
            Message::Account(message) => self
                .accounts
                .get_updates(message, &self.accounts_data_slice),
            Message::Slot(message) => self.slots.get_updates(message, commitment),
            Message::Transaction(message) => {
                let mut updates = self.transactions.get_updates(message);
                updates.append(&mut self.transactions_status.get_updates(message));
                updates
            }
            Message::DeshredTransaction(_) => FilteredUpdates::new(),
            Message::Entry(message) => self.entries.get_updates(message),
            Message::Block(message) => self.blocks.get_updates(message, &self.accounts_data_slice),
            Message::BlockMeta(message) => self.blocks_meta.get_updates(message),
        }
    }

    pub fn get_pong_msg(&self) -> Option<FilteredUpdate> {
        self.ping
            .map(|id| FilteredUpdate::new_empty(FilteredUpdateOneof::pong(id)))
    }
}

#[derive(Debug, Default, Clone)]
struct FilterAccounts {
    nonempty_txn_signature: Vec<(FilterName, Option<bool>)>,
    nonempty_txn_signature_required: HashSet<FilterName>,
    account: HashMap<Pubkey, HashSet<FilterName>>,
    account_required: HashSet<FilterName>,
    owner: HashMap<Pubkey, HashSet<FilterName>>,
    owner_required: HashSet<FilterName>,
    filters: Vec<(FilterName, FilterAccountsState)>,
}

impl FilterAccounts {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterAccounts>,
        limits: &FilterLimitsAccounts,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        let mut this = Self::default();
        for (name, filter) in configs {
            this.nonempty_txn_signature
                .push((names.get(name)?, filter.nonempty_txn_signature));
            if filter.nonempty_txn_signature.is_some() {
                this.nonempty_txn_signature_required
                    .insert(names.get(name)?);
            }

            FilterLimits::check_any(
                filter.account.is_empty() && filter.owner.is_empty(),
                limits.any,
            )?;
            FilterLimits::check_pubkey_max(filter.account.len(), limits.account_max)?;
            FilterLimits::check_pubkey_max(filter.owner.len(), limits.owner_max)?;

            Self::set(
                &mut this.account,
                &mut this.account_required,
                name,
                names,
                Filter::decode_pubkeys(&filter.account, &limits.account_reject),
            )?;

            Self::set(
                &mut this.owner,
                &mut this.owner_required,
                name,
                names,
                Filter::decode_pubkeys(&filter.owner, &limits.owner_reject),
            )?;

            this.filters
                .push((names.get(name)?, FilterAccountsState::new(&filter.filters)?));
        }
        Ok(this)
    }

    fn set(
        map: &mut HashMap<Pubkey, HashSet<FilterName>>,
        map_required: &mut HashSet<FilterName>,
        name: &str,
        names: &mut FilterNames,
        keys: impl Iterator<Item = FilterResult<Pubkey>>,
    ) -> FilterResult<bool> {
        let mut required = false;
        for maybe_key in keys {
            if map.entry(maybe_key?).or_default().insert(names.get(name)?) {
                required = true;
            }
        }

        if required {
            map_required.insert(names.get(name)?);
        }
        Ok(required)
    }

    fn collect_rules(&self, rules: &mut Vec<AccountFilterRule>) {
        let mut rules_by_name =
            HashMap::<FilterName, AccountFilterRule>::with_capacity(self.filters.len());
        for (name, _state) in self.filters.iter() {
            rules_by_name.insert(
                name.clone(),
                AccountFilterRule {
                    nonempty_txn_signature: None,
                    account: HashSet::new(),
                    account_required: self.account_required.contains(name),
                    owner: HashSet::new(),
                    owner_required: self.owner_required.contains(name),
                },
            );
        }

        for (name, nonempty_txn_signature) in self.nonempty_txn_signature.iter() {
            if let Some(rule) = rules_by_name.get_mut(name) {
                rule.nonempty_txn_signature = *nonempty_txn_signature;
            }
        }

        for (pubkey, names) in self.account.iter() {
            for name in names.iter() {
                if let Some(rule) = rules_by_name.get_mut(name) {
                    rule.account.insert(*pubkey);
                }
            }
        }

        for (pubkey, names) in self.owner.iter() {
            for name in names.iter() {
                if let Some(rule) = rules_by_name.get_mut(name) {
                    rule.owner.insert(*pubkey);
                }
            }
        }

        for (name, _state) in self.filters.iter() {
            if let Some(rule) = rules_by_name.remove(name) {
                rules.push(rule);
            }
        }
    }

    fn get_updates(
        &self,
        message: &MessageAccount,
        accounts_data_slice: &FilterAccountsDataSlice,
    ) -> FilteredUpdates {
        let mut filter = FilterAccountsMatch::new(self);
        filter.match_txn_signature(&message.account.txn_signature);
        filter.match_account(&message.account.pubkey);
        filter.match_owner(&message.account.owner);
        filter.match_data_lamports(&message.account.data, message.account.lamports);
        let filters = filter.get_filters();
        filtered_updates_once_owned!(
            filters,
            FilteredUpdateOneof::account(message, accounts_data_slice.clone()),
            message.created_at
        )
    }
}

#[derive(Debug, Default, Clone)]
struct FilterAccountsState {
    memcmp: Vec<(usize, Vec<u8>)>,
    datasize: Option<usize>,
    token_account_state: bool,
    lamports: Vec<FilterAccountsLamports>,
}

impl FilterAccountsState {
    fn new(filters: &[SubscribeRequestFilterAccountsFilter]) -> FilterResult<Self> {
        const MAX_FILTERS: usize = 4;
        const MAX_DATA_SIZE: usize = 128;
        const MAX_DATA_BASE58_SIZE: usize = 175;
        const MAX_DATA_BASE64_SIZE: usize = 172;

        if filters.len() > MAX_FILTERS {
            return Err(FilterError::CreateAccountStateMaxFilters { max: MAX_FILTERS });
        }

        let mut this = Self::default();
        for filter in filters {
            match &filter.filter {
                Some(AccountsFilterDataOneof::Memcmp(memcmp)) => {
                    let data = match &memcmp.data {
                        Some(AccountsFilterMemcmpOneof::Bytes(data)) => data.clone(),
                        Some(AccountsFilterMemcmpOneof::Base58(data)) => {
                            if data.len() > MAX_DATA_BASE58_SIZE {
                                return Err(FilterError::CreateAccountState("data too large"));
                            }
                            bs58::decode(data)
                                .into_vec()
                                .map_err(|_| FilterError::CreateAccountState("invalid base58"))?
                        }
                        Some(AccountsFilterMemcmpOneof::Base64(data)) => {
                            if data.len() > MAX_DATA_BASE64_SIZE {
                                return Err(FilterError::CreateAccountState("data too large"));
                            }
                            base64_engine
                                .decode(data)
                                .map_err(|_| FilterError::CreateAccountState("invalid base64"))?
                        }
                        None => {
                            return Err(FilterError::CreateAccountState(
                                "data for memcmp should be defined",
                            ))
                        }
                    };
                    if data.len() > MAX_DATA_SIZE {
                        return Err(FilterError::CreateAccountState("data too large"));
                    }
                    this.memcmp.push((memcmp.offset as usize, data));
                }
                Some(AccountsFilterDataOneof::Datasize(datasize)) => {
                    if this.datasize.replace(*datasize as usize).is_some() {
                        return Err(FilterError::CreateAccountState(
                            "datasize used more than once",
                        ));
                    }
                }
                Some(AccountsFilterDataOneof::TokenAccountState(value)) => {
                    if !value {
                        return Err(FilterError::CreateAccountState(
                            "token_account_state only allowed to be true",
                        ));
                    }
                    this.token_account_state = true;
                }
                Some(AccountsFilterDataOneof::Lamports(
                    SubscribeRequestFilterAccountsFilterLamports { cmp },
                )) => {
                    let Some(cmp) = cmp else {
                        return Err(FilterError::CreateAccountState(
                            "cmp for lamports should be defined",
                        ));
                    };
                    this.lamports.push(cmp.into());
                }
                None => {
                    return Err(FilterError::CreateAccountState("filter should be defined"));
                }
            }
        }
        Ok(this)
    }

    fn is_empty(&self) -> bool {
        self.memcmp.is_empty()
            && self.datasize.is_none()
            && !self.token_account_state
            && self.lamports.is_empty()
    }

    fn is_match(&self, data: &[u8], lamports: u64) -> bool {
        if matches!(self.datasize, Some(datasize) if data.len() != datasize) {
            return false;
        }
        if self.token_account_state && !TokenAccount::valid_account_data(data) {
            return false;
        }
        if self.lamports.iter().any(|f| !f.is_match(lamports)) {
            return false;
        }
        for (offset, bytes) in self.memcmp.iter() {
            if data.len() < *offset + bytes.len() {
                return false;
            }
            let data = &data[*offset..*offset + bytes.len()];
            if data != bytes {
                return false;
            }
        }
        true
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterAccountsLamports {
    Eq(u64),
    Ne(u64),
    Lt(u64),
    Gt(u64),
}

impl From<&AccountsFilterLamports> for FilterAccountsLamports {
    fn from(cmp: &AccountsFilterLamports) -> Self {
        match cmp {
            AccountsFilterLamports::Eq(value) => Self::Eq(*value),
            AccountsFilterLamports::Ne(value) => Self::Ne(*value),
            AccountsFilterLamports::Lt(value) => Self::Lt(*value),
            AccountsFilterLamports::Gt(value) => Self::Gt(*value),
        }
    }
}

impl FilterAccountsLamports {
    const fn is_match(self, lamports: u64) -> bool {
        match self {
            Self::Eq(value) => value == lamports,
            Self::Ne(value) => value != lamports,
            Self::Lt(value) => value > lamports,
            Self::Gt(value) => value < lamports,
        }
    }
}

#[derive(Debug)]
struct FilterAccountsMatch<'a> {
    filter: &'a FilterAccounts,
    nonempty_txn_signature: HashSet<&'a str>,
    account: HashSet<&'a str>,
    owner: HashSet<&'a str>,
    data: HashSet<&'a str>,
}

impl<'a> FilterAccountsMatch<'a> {
    fn new(filter: &'a FilterAccounts) -> Self {
        Self {
            filter,
            nonempty_txn_signature: Default::default(),
            account: Default::default(),
            owner: Default::default(),
            data: Default::default(),
        }
    }

    fn extend(
        set: &mut HashSet<&'a str>,
        map: &'a HashMap<Pubkey, HashSet<FilterName>>,
        key: &Pubkey,
    ) {
        if let Some(names) = map.get(key) {
            for name in names {
                set.insert(name.as_ref());
            }
        }
    }

    fn match_txn_signature(&mut self, txn_signature: &Option<Signature>) {
        for (name, filter) in self.filter.nonempty_txn_signature.iter() {
            if let Some(nonempty_txn_signature) = filter {
                if *nonempty_txn_signature == txn_signature.is_some() {
                    self.nonempty_txn_signature.insert(name.as_ref());
                }
            }
        }
    }

    fn match_account(&mut self, pubkey: &Pubkey) {
        Self::extend(&mut self.account, &self.filter.account, pubkey)
    }

    fn match_owner(&mut self, pubkey: &Pubkey) {
        Self::extend(&mut self.owner, &self.filter.owner, pubkey)
    }

    fn match_data_lamports(&mut self, data: &[u8], lamports: u64) {
        for (name, filter) in self.filter.filters.iter() {
            if filter.is_match(data, lamports) {
                self.data.insert(name.as_ref());
            }
        }
    }

    fn get_filters(&self) -> FilteredUpdateFilters {
        self.filter
            .filters
            .iter()
            .filter_map(|(filter_name, filter)| {
                let name = filter_name.as_ref();
                let af = &self.filter;

                // If filter name in required but not in matched => return `false`
                if af.nonempty_txn_signature_required.contains(name)
                    && !self.nonempty_txn_signature.contains(name)
                {
                    return None;
                }
                if af.account_required.contains(name) && !self.account.contains(name) {
                    return None;
                }
                if af.owner_required.contains(name) && !self.owner.contains(name) {
                    return None;
                }
                if !filter.is_empty() && !self.data.contains(name) {
                    return None;
                }

                Some(filter_name.clone())
            })
            .collect()
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct FilterSlotsInner {
    filter_by_commitment: bool,
    interslot_updates: bool,
}

impl FilterSlotsInner {
    fn new(filter: SubscribeRequestFilterSlots) -> Self {
        Self {
            filter_by_commitment: filter.filter_by_commitment.unwrap_or_default(),
            interslot_updates: filter.interslot_updates.unwrap_or_default(),
        }
    }
}

#[derive(Debug, Default, Clone)]
struct FilterSlots {
    filters: HashMap<FilterName, FilterSlotsInner>,
}

impl FilterSlots {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterSlots>,
        limits: &FilterLimitsSlots,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        Ok(Self {
            filters: configs
                .iter()
                .map(|(name, filter)| {
                    names
                        .get(name)
                        .map(|name| (name, FilterSlotsInner::new(*filter)))
                })
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_updates(
        &self,
        message: &MessageSlot,
        commitment: Option<CommitmentLevel>,
    ) -> FilteredUpdates {
        let filters = self
            .filters
            .iter()
            .filter_map(|(name, inner)| {
                if (!inner.filter_by_commitment
                    || commitment
                        .map(|commitment| commitment == message.status)
                        .unwrap_or(false))
                    && (inner.interslot_updates
                        || matches!(
                            message.status,
                            SlotStatus::Processed | SlotStatus::Confirmed | SlotStatus::Finalized
                        ))
                {
                    Some(name.clone())
                } else {
                    None
                }
            })
            .collect::<FilteredUpdateFilters>();
        filtered_updates_once_owned!(
            filters,
            FilteredUpdateOneof::slot(message.clone()),
            message.created_at
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FilterTransactionsType {
    Transaction,
    TransactionStatus,
}

#[derive(Debug, Clone)]
struct FilterTransactionsInner {
    vote: Option<bool>,
    failed: Option<bool>,
    signature: Option<Signature>,
    account_include: HashSet<Pubkey>,
    account_exclude: HashSet<Pubkey>,
    account_required: HashSet<Pubkey>,
}

#[derive(Debug, Clone)]
struct FilterTransactions {
    filter_type: FilterTransactionsType,
    filters: HashMap<FilterName, FilterTransactionsInner>,
}

impl FilterTransactions {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterTransactions>,
        limits: &FilterLimitsTransactions,
        filter_type: FilterTransactionsType,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        let mut filters = HashMap::new();
        for (name, filter) in configs {
            FilterLimits::check_any(
                filter.vote.is_none()
                    && filter.failed.is_none()
                    && filter.account_include.is_empty()
                    && filter.account_exclude.is_empty()
                    && filter.account_required.is_empty(),
                limits.any,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_include.len(),
                limits.account_include_max,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_exclude.len(),
                limits.account_exclude_max,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_required.len(),
                limits.account_required_max,
            )?;

            filters.insert(
                names.get(name)?,
                FilterTransactionsInner {
                    vote: filter.vote,
                    failed: filter.failed,
                    signature: filter
                        .signature
                        .as_ref()
                        .map(|signature_str| {
                            signature_str.parse().map_err(FilterError::InvalidSignature)
                        })
                        .transpose()?,
                    account_include: Filter::decode_pubkeys_into_set(
                        &filter.account_include,
                        &limits.account_include_reject,
                    )?,
                    account_exclude: Filter::decode_pubkeys_into_set(
                        &filter.account_exclude,
                        &HashSet::new(),
                    )?,
                    account_required: Filter::decode_pubkeys_into_set(
                        &filter.account_required,
                        &HashSet::new(),
                    )?,
                },
            );
        }
        Ok(Self {
            filter_type,
            filters,
        })
    }

    fn collect_rules(&self, rules: &mut Vec<TransactionFilterRule>) {
        rules.extend(self.filters.values().map(TransactionFilterRule::from_inner));
    }

    pub fn get_updates(&self, message: &MessageTransaction) -> FilteredUpdates {
        let filters = self
            .filters
            .iter()
            .filter_map(|(name, inner)| {
                if let Some(is_vote) = inner.vote {
                    if is_vote != message.transaction.is_vote {
                        return None;
                    }
                }

                if let Some(is_failed) = inner.failed {
                    if is_failed != message.transaction.meta.err.is_some() {
                        return None;
                    }
                }

                if let Some(signature) = &inner.signature {
                    let tx_sig = message.transaction.transaction.signatures.first();
                    if Some(signature.as_ref()) != tx_sig.map(|sig| sig.as_ref()) {
                        return None;
                    }
                }

                if !inner.account_include.is_empty()
                    && inner
                        .account_include
                        .intersection(&message.transaction.account_keys)
                        .next()
                        .is_none()
                {
                    return None;
                }

                if !inner.account_exclude.is_empty()
                    && inner
                        .account_exclude
                        .intersection(&message.transaction.account_keys)
                        .next()
                        .is_some()
                {
                    return None;
                }

                if !inner.account_required.is_empty()
                    && !inner
                        .account_required
                        .is_subset(&message.transaction.account_keys)
                {
                    return None;
                }

                Some(name.clone())
            })
            .collect::<FilteredUpdateFilters>();

        filtered_updates_once_owned!(
            filters,
            match self.filter_type {
                FilterTransactionsType::Transaction => FilteredUpdateOneof::transaction(message),
                FilterTransactionsType::TransactionStatus => {
                    FilteredUpdateOneof::transaction_status(message)
                }
            },
            message.created_at
        )
    }
}

#[derive(Debug, Clone)]
struct FilterDeshredTransactionsInner {
    vote: Option<bool>,
    account_include: HashSet<Pubkey>,
    account_exclude: HashSet<Pubkey>,
    account_required: HashSet<Pubkey>,
}

#[derive(Debug, Default, Clone)]
struct FilterDeshredTransactions {
    filters: HashMap<FilterName, FilterDeshredTransactionsInner>,
}

impl FilterDeshredTransactions {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterDeshredTransactions>,
        limits: &FilterLimitsDeshredTransactions,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        let mut filters = HashMap::new();
        for (name, filter) in configs {
            FilterLimits::check_any(
                filter.vote.is_none()
                    && filter.account_include.is_empty()
                    && filter.account_exclude.is_empty()
                    && filter.account_required.is_empty(),
                limits.any,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_include.len(),
                limits.account_include_max,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_exclude.len(),
                limits.account_exclude_max,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_required.len(),
                limits.account_required_max,
            )?;

            filters.insert(
                names.get(name)?,
                FilterDeshredTransactionsInner {
                    vote: filter.vote,
                    account_include: Filter::decode_pubkeys_into_set(
                        &filter.account_include,
                        &limits.account_include_reject,
                    )?,
                    account_exclude: Filter::decode_pubkeys_into_set(
                        &filter.account_exclude,
                        &HashSet::new(),
                    )?,
                    account_required: Filter::decode_pubkeys_into_set(
                        &filter.account_required,
                        &HashSet::new(),
                    )?,
                },
            );
        }
        Ok(Self { filters })
    }

    fn collect_rules(&self, rules: &mut Vec<DeshredTransactionFilterRule>) {
        rules.extend(
            self.filters
                .values()
                .map(DeshredTransactionFilterRule::from_inner),
        );
    }

    pub fn get_updates(&self, message: &MessageDeshredTransaction) -> FilteredUpdatesDeshred {
        let filters = self
            .filters
            .iter()
            .filter_map(|(name, inner)| {
                if let Some(is_vote) = inner.vote {
                    if is_vote != message.transaction.is_vote {
                        return None;
                    }
                }

                let tx = &message.transaction;

                if !inner.account_include.is_empty()
                    && !tx
                        .all_account_keys()
                        .any(|key| inner.account_include.contains(key))
                {
                    return None;
                }

                if !inner.account_exclude.is_empty()
                    && tx
                        .all_account_keys()
                        .any(|key| inner.account_exclude.contains(key))
                {
                    return None;
                }

                if !inner.account_required.is_empty() {
                    let all_keys: HashSet<&Pubkey> = tx.all_account_keys().collect();
                    if !inner
                        .account_required
                        .iter()
                        .all(|key| all_keys.contains(key))
                    {
                        return None;
                    }
                }

                Some(name.clone())
            })
            .collect::<FilteredUpdateFilters>();

        let mut messages = FilteredUpdatesDeshred::new();
        if !filters.is_empty() {
            messages.push(FilteredUpdateDeshred::new(
                filters,
                FilteredUpdateDeshredOneof::deshred_transaction(message),
                message.created_at,
            ));
        }
        messages
    }
}

/// Filter for the SubscribeDeshred RPC endpoint.
/// Handles deshred transaction subscriptions separately from the main Subscribe RPC.
#[derive(Debug, Clone, Default)]
pub struct DeshredFilter {
    deshred_transactions: FilterDeshredTransactions,
    ping: Option<i32>,
}

impl DeshredFilter {
    pub fn new(
        config: &SubscribeDeshredRequest,
        limits: &FilterLimits,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        Ok(Self {
            deshred_transactions: FilterDeshredTransactions::new(
                &config.deshred_transactions,
                &limits.deshred_transactions,
                names,
            )?,
            ping: config.ping.as_ref().map(|msg| msg.id),
        })
    }

    pub fn get_updates(&self, message: &Message) -> FilteredUpdatesDeshred {
        match message {
            Message::DeshredTransaction(message) => self.deshred_transactions.get_updates(message),
            _ => FilteredUpdatesDeshred::new(),
        }
    }

    pub fn get_deshred_transaction_filter_rules(&self) -> Vec<DeshredTransactionFilterRule> {
        let mut rules = Vec::with_capacity(self.deshred_transactions.filters.len());
        self.deshred_transactions.collect_rules(&mut rules);
        rules
    }

    pub fn get_pong_msg(&self) -> Option<FilteredUpdateDeshred> {
        self.ping.map(FilteredUpdateDeshred::pong)
    }

    pub fn get_metrics(&self) -> [(&'static str, usize); 2] {
        [
            (
                "deshred_transactions",
                self.deshred_transactions.filters.len(),
            ),
            ("all", self.deshred_transactions.filters.len()),
        ]
    }
}

#[derive(Debug, Default, Clone)]
struct FilterEntries {
    filters: Vec<FilterName>,
}

impl FilterEntries {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterEntry>,
        limits: &FilterLimitsEntries,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        Ok(Self {
            filters: configs
                .iter()
                .map(|(name, _filter)| names.get(name))
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_updates(&self, message: &Arc<MessageEntry>) -> FilteredUpdates {
        let filters = self.filters.as_slice();
        filtered_updates_once_ref!(
            filters,
            FilteredUpdateOneof::entry(Arc::clone(message)),
            message.created_at
        )
    }
}

#[derive(Debug, Clone)]
struct FilterBlocksInner {
    account_include: HashSet<Pubkey>,
    include_transactions: Option<bool>,
    include_accounts: Option<bool>,
    include_entries: Option<bool>,
}

#[derive(Debug, Default, Clone)]
struct FilterBlocks {
    filters: HashMap<FilterName, FilterBlocksInner>,
}

impl FilterBlocks {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterBlocks>,
        limits: &FilterLimitsBlocks,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        let mut this = Self::default();
        for (name, filter) in configs {
            FilterLimits::check_any(
                filter.account_include.is_empty(),
                limits.account_include_any,
            )?;
            FilterLimits::check_pubkey_max(
                filter.account_include.len(),
                limits.account_include_max,
            )?;
            if !(filter.include_transactions == Some(false) || limits.include_transactions) {
                return Err(FilterError::CreateBlocksNotAllowed("transactions"));
            }
            if !(matches!(filter.include_accounts, None | Some(false)) || limits.include_accounts) {
                return Err(FilterError::CreateBlocksNotAllowed("accounts"));
            }
            if !(matches!(filter.include_entries, None | Some(false)) || limits.include_accounts) {
                return Err(FilterError::CreateBlocksNotAllowed("entries"));
            }

            this.filters.insert(
                names.get(name)?,
                FilterBlocksInner {
                    account_include: Filter::decode_pubkeys_into_set(
                        &filter.account_include,
                        &limits.account_include_reject,
                    )?,
                    include_transactions: filter.include_transactions,
                    include_accounts: filter.include_accounts,
                    include_entries: filter.include_entries,
                },
            );
        }
        Ok(this)
    }

    fn get_updates(
        &self,
        message: &Arc<MessageBlock>,
        accounts_data_slice: &FilterAccountsDataSlice,
    ) -> FilteredUpdates {
        let mut updates = FilteredUpdates::new();
        for (filter, inner) in self.filters.iter() {
            #[allow(clippy::unnecessary_filter_map)]
            let transactions = if matches!(inner.include_transactions, None | Some(true)) {
                message
                    .transactions
                    .iter()
                    .filter_map(|tx| {
                        if !inner.account_include.is_empty()
                            && inner
                                .account_include
                                .intersection(&tx.account_keys)
                                .next()
                                .is_none()
                        {
                            None
                        } else {
                            Some(Arc::clone(tx))
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            };

            #[allow(clippy::unnecessary_filter_map)]
            let accounts = if inner.include_accounts == Some(true) {
                message
                    .accounts
                    .iter()
                    .filter_map(|account| {
                        if !inner.account_include.is_empty()
                            && !inner.account_include.contains(&account.pubkey)
                        {
                            None
                        } else {
                            Some(Arc::clone(account))
                        }
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            };

            let entries = if inner.include_entries == Some(true) {
                message.entries.to_vec()
            } else {
                vec![]
            };

            let mut filters = FilteredUpdateFilters::new();
            filters.push(filter.clone());
            updates.push(FilteredUpdate::new(
                filters,
                FilteredUpdateOneof::block(Box::new(FilteredUpdateBlock {
                    meta: Arc::clone(&message.meta),
                    transactions,
                    updated_account_count: message.updated_account_count,
                    accounts_data_slice: accounts_data_slice.clone(),
                    accounts,
                    entries,
                })),
                message.created_at,
            ));
        }
        updates
    }
}

#[derive(Debug, Default, Clone)]
struct FilterBlocksMeta {
    filters: Vec<FilterName>,
}

impl FilterBlocksMeta {
    fn new(
        configs: &HashMap<String, SubscribeRequestFilterBlocksMeta>,
        limits: &FilterLimitsBlocksMeta,
        names: &mut FilterNames,
    ) -> FilterResult<Self> {
        FilterLimits::check_max(configs.len(), limits.max)?;

        Ok(Self {
            filters: configs
                .iter()
                .map(|(name, _filter)| names.get(name))
                .collect::<Result<_, _>>()?,
        })
    }

    fn get_updates(&self, message: &Arc<MessageBlockMeta>) -> FilteredUpdates {
        let filters = self.filters.as_slice();
        filtered_updates_once_ref!(
            filters,
            FilteredUpdateOneof::block_meta(Arc::clone(message)),
            message.created_at
        )
    }
}

#[derive(Debug, Clone, Default, PartialEq)]
pub struct FilterAccountsDataSlice(Arc<[Range<usize>]>);

impl AsRef<[Range<usize>]> for FilterAccountsDataSlice {
    #[inline]
    fn as_ref(&self) -> &[Range<usize>] {
        &self.0
    }
}

impl FilterAccountsDataSlice {
    pub fn new(slices: &[SubscribeRequestAccountsDataSlice], limits: usize) -> FilterResult<Self> {
        FilterLimits::check_max(slices.len(), limits)?;

        let slices = slices
            .iter()
            .map(|s| Range {
                start: s.offset as usize,
                end: (s.offset + s.length) as usize,
            })
            .collect::<Vec<_>>();

        for (i, slice_a) in slices.iter().enumerate() {
            // check order
            for slice_b in slices[i + 1..].iter() {
                if slice_a.start > slice_b.start {
                    return Err(FilterError::CreateDataSliceOutOfOrder);
                }
            }

            // check overlap
            for slice_b in slices[0..i].iter() {
                if slice_a.start < slice_b.end {
                    return Err(FilterError::CreateDataSliceOverlap);
                }
            }
        }

        Ok(Self::new_unchecked(Arc::from(slices.into_boxed_slice())))
    }

    pub const fn new_unchecked(slices: Arc<[Range<usize>]>) -> Self {
        Self(slices)
    }

    pub fn get_slice(&self, source: &[u8]) -> Vec<u8> {
        if self.0.is_empty() {
            source.to_vec()
        } else {
            // Make sure the vec capacity fit exaclty the data we want to copy
            // Why: fitting capacity to length avoid reallocation if we ever need to promote the vector to `Bytes`.
            let mut data = Vec::with_capacity(
                self.0
                    .iter()
                    .filter(|range| source.len() > range.end)
                    .map(|ds| ds.end - ds.start)
                    .sum(),
            );
            for data_slice in self.0.iter() {
                if source.len() >= data_slice.end {
                    data.extend_from_slice(&source[data_slice.start..data_slice.end]);
                }
            }
            data
        }
    }

    pub fn get_slice_len(&self, source: &[u8]) -> usize {
        if self.0.is_empty() {
            source.len()
        } else {
            let mut len = 0;
            for slice in self.0.iter() {
                if source.len() >= slice.end {
                    len += source[slice.start..slice.end].len();
                }
            }
            len
        }
    }

    pub fn slice_encode_raw(&self, tag: u32, source: &[u8], buf: &mut impl BufMut) {
        let len = self.get_slice_len(source) as u64;
        if len > 0 {
            encode_key(tag, WireType::LengthDelimited, buf);
            encode_varint(len, buf);

            if self.0.is_empty() {
                buf.put_slice(source);
            } else {
                for data_slice in self.0.iter() {
                    if source.len() >= data_slice.end {
                        buf.put_slice(&source[data_slice.start..data_slice.end]);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use {
        super::{DeshredFilter, Filter},
        crate::plugin::{
            convert_to,
            filter::{
                limits::FilterLimits,
                message::{FilteredUpdateDeshredOneof, FilteredUpdateFilters, FilteredUpdateOneof},
                name::{FilterName, FilterNames},
            },
            message::{
                Message, MessageDeshredTransaction, MessageDeshredTransactionInfo,
                MessageTransaction, MessageTransactionInfo,
            },
        },
        prost_types::Timestamp,
        solana_hash::Hash,
        solana_keypair::Keypair,
        solana_message::{v0::LoadedAddresses, Message as SolMessage, MessageHeader},
        solana_pubkey::Pubkey,
        solana_signer::Signer,
        solana_transaction::{versioned::VersionedTransaction, Transaction},
        solana_transaction_status::TransactionStatusMeta,
        std::{
            collections::HashMap,
            sync::{Arc, OnceLock},
            time::{Duration, SystemTime},
        },
        yellowstone_grpc_proto::geyser::{
            SubscribeDeshredRequest, SubscribeRequest, SubscribeRequestFilterAccounts,
            SubscribeRequestFilterDeshredTransactions, SubscribeRequestFilterTransactions,
            SubscribeRequestPing,
        },
    };

    fn create_filter_names() -> FilterNames {
        FilterNames::new(64, 1024, Duration::from_secs(1))
    }

    fn create_message_transaction(
        keypair: &Keypair,
        account_keys: Vec<Pubkey>,
    ) -> MessageTransaction {
        let message = SolMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys,
            ..SolMessage::default()
        };
        let recent_blockhash = Hash::default();
        let versioned_transaction =
            VersionedTransaction::from(Transaction::new(&[keypair], message, recent_blockhash));
        let meta = convert_to::create_transaction_meta(&TransactionStatusMeta {
            status: Ok(()),
            fee: 0,
            pre_balances: vec![],
            post_balances: vec![],
            inner_instructions: None,
            log_messages: None,
            pre_token_balances: None,
            post_token_balances: None,
            rewards: None,
            loaded_addresses: LoadedAddresses::default(),
            return_data: None,
            compute_units_consumed: None,
            cost_units: None,
        });
        let sig = versioned_transaction
            .signatures
            .first()
            .expect("No signature found");
        let account_keys = versioned_transaction
            .message
            .static_account_keys()
            .iter()
            .copied()
            .collect();
        MessageTransaction {
            transaction: Arc::new(MessageTransactionInfo {
                signature: *sig,
                is_vote: true,
                transaction: convert_to::create_transaction(&versioned_transaction),
                meta,
                index: 1,
                account_keys,
                pre_encoded: OnceLock::new(),
            }),
            slot: 100,
            created_at: Timestamp::from(SystemTime::now()),
        }
    }

    #[test]
    fn test_filters_all_empty() {
        // ensure Filter can be created with empty values
        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: HashMap::new(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        };
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names());
        assert!(filter.is_ok());
    }

    #[test]
    fn test_filters_account_empty() {
        let mut accounts = HashMap::new();

        accounts.insert(
            "solend".to_owned(),
            SubscribeRequestFilterAccounts {
                nonempty_txn_signature: None,
                account: vec![],
                owner: vec![],
                filters: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts,
            slots: HashMap::new(),
            transactions: HashMap::new(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        };
        let mut limit = FilterLimits::default();
        limit.accounts.any = false;
        let filter = Filter::new(&config, &limit, &mut create_filter_names());
        // filter should fail
        assert!(filter.is_err());
    }

    #[test]
    fn test_filters_transaction_empty() {
        let mut transactions = HashMap::new();

        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        };
        let mut limit = FilterLimits::default();
        limit.transactions.any = false;
        let filter = Filter::new(&config, &limit, &mut create_filter_names());
        // filter should fail
        assert!(filter.is_err());
    }

    #[test]
    fn test_filters_transaction_not_null() {
        let mut transactions = HashMap::new();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: Some(true),
                failed: None,
                signature: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        };
        let mut limit = FilterLimits::default();
        limit.transactions.any = false;
        let filter_res = Filter::new(&config, &limit, &mut create_filter_names());
        // filter should succeed
        assert!(filter_res.is_ok());
    }

    #[test]
    fn test_transaction_include_a() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_include = [account_key_a].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let mut config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: transactions.clone(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        };
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[0].message,
            FilteredUpdateOneof::Transaction(_)
        ));

        config.transactions_status = transactions;
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[1].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[1].message,
            FilteredUpdateOneof::TransactionStatus(_)
        ));
    }

    #[test]
    fn test_transaction_include_b() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_include = [account_key_b].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let mut config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: transactions.clone(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        };
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[0].message,
            FilteredUpdateOneof::Transaction(_)
        ));

        config.transactions_status = transactions;
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[1].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[1].message,
            FilteredUpdateOneof::TransactionStatus(_)
        ));
    }

    #[test]
    fn test_transaction_exclude() {
        let mut transactions = HashMap::new();

        let keypair_a = Keypair::new();
        let account_key_a = keypair_a.pubkey();
        let keypair_b = Keypair::new();
        let account_key_b = keypair_b.pubkey();
        let account_exclude = [account_key_b].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include: vec![],
                account_exclude,
                account_required: vec![],
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        };
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_b, vec![account_key_b, account_key_a]);
        let message = Message::Transaction(message_transaction);
        for message in filter.get_updates(&message, None) {
            assert!(message.filters.is_empty());
        }
    }

    #[test]
    fn test_transaction_required_x_include_y_z_case001() {
        let mut transactions = HashMap::new();

        let keypair_x = Keypair::new();
        let account_key_x = keypair_x.pubkey();
        let account_key_y = Pubkey::new_unique();
        let account_key_z = Pubkey::new_unique();

        // require x, include y, z
        let account_include = [account_key_y, account_key_z]
            .iter()
            .map(|k| k.to_string())
            .collect();
        let account_required = [account_key_x].iter().map(|k| k.to_string()).collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required,
            },
        );

        let mut config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions: transactions.clone(),
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        };
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction = create_message_transaction(
            &keypair_x,
            vec![account_key_x, account_key_y, account_key_z],
        );
        let message = Message::Transaction(message_transaction);
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 1);
        assert_eq!(
            updates[0].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[0].message,
            FilteredUpdateOneof::Transaction(_)
        ));

        config.transactions_status = transactions;
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();
        let updates = filter.get_updates(&message, None);
        assert_eq!(updates.len(), 2);
        assert_eq!(
            updates[1].filters,
            FilteredUpdateFilters::from_vec(vec![FilterName::new("serum")])
        );
        assert!(matches!(
            updates[1].message,
            FilteredUpdateOneof::TransactionStatus(_)
        ));
    }

    fn create_message_deshred_transaction(
        keypair: &Keypair,
        static_account_keys: Vec<Pubkey>,
        loaded_writable: Vec<Pubkey>,
        loaded_readonly: Vec<Pubkey>,
        is_vote: bool,
    ) -> MessageDeshredTransaction {
        let message = SolMessage {
            header: MessageHeader {
                num_required_signatures: 1,
                ..MessageHeader::default()
            },
            account_keys: static_account_keys.clone(),
            ..SolMessage::default()
        };
        let recent_blockhash = Hash::default();
        let versioned_transaction =
            VersionedTransaction::from(Transaction::new(&[keypair], message, recent_blockhash));

        MessageDeshredTransaction {
            transaction: Arc::new(MessageDeshredTransactionInfo {
                signature: *versioned_transaction
                    .signatures
                    .first()
                    .expect("No signature found"),
                is_vote,
                transaction: convert_to::create_transaction(&versioned_transaction),
                static_account_keys: static_account_keys.into_iter().collect(),
                loaded_writable_addresses: loaded_writable,
                loaded_readonly_addresses: loaded_readonly,
            }),
            slot: 100,
            created_at: Timestamp::from(SystemTime::now()),
        }
    }

    #[test]
    fn test_deshred_filter_empty_rejects() {
        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
        };
        let mut limit = FilterLimits::default();
        limit.deshred_transactions.any = false;
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names());
        assert!(filter.is_err());
    }

    #[test]
    fn test_deshred_filter_vote() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();

        // Filter: vote=true
        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "votes".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: Some(true),
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        // Vote transaction should match
        let msg_vote =
            create_message_deshred_transaction(&keypair, vec![key_a], vec![], vec![], true);
        let message = Message::DeshredTransaction(msg_vote);
        let updates = filter.get_updates(&message);
        assert_eq!(updates.len(), 1);
        assert!(matches!(
            updates[0].message,
            FilteredUpdateDeshredOneof::DeshredTransaction(_)
        ));

        // Non-vote transaction should not match
        let msg_nonvote =
            create_message_deshred_transaction(&keypair, vec![key_a], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg_nonvote);
        let updates = filter.get_updates(&message);
        assert!(updates.is_empty());
    }

    #[test]
    fn test_deshred_filter_account_include_static() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_b = Pubkey::new_unique();
        let key_c = Pubkey::new_unique();

        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![key_b.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        // Transaction with key_b in static keys should match
        let msg =
            create_message_deshred_transaction(&keypair, vec![key_a, key_b], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message);
        assert_eq!(updates.len(), 1);

        // Transaction without key_b should not match
        let msg =
            create_message_deshred_transaction(&keypair, vec![key_a, key_c], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message);
        assert!(updates.is_empty());
    }

    #[test]
    fn test_deshred_filter_account_include_loaded_addresses() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_alt_w = Pubkey::new_unique();
        let key_alt_r = Pubkey::new_unique();

        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![key_alt_w.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        // key_alt_w in loaded writable should match
        let msg = create_message_deshred_transaction(
            &keypair,
            vec![key_a],
            vec![key_alt_w],
            vec![],
            false,
        );
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message);
        assert_eq!(updates.len(), 1);

        // key_alt_r in loaded readonly should match when filtering for it
        let mut deshred_transactions2 = HashMap::new();
        deshred_transactions2.insert(
            "f2".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![key_alt_r.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );
        let config2 = SubscribeDeshredRequest {
            deshred_transactions: deshred_transactions2,
            ping: None,
        };
        let filter2 = DeshredFilter::new(&config2, &limit, &mut create_filter_names()).unwrap();

        let msg = create_message_deshred_transaction(
            &keypair,
            vec![key_a],
            vec![],
            vec![key_alt_r],
            false,
        );
        let message = Message::DeshredTransaction(msg);
        let updates = filter2.get_updates(&message);
        assert_eq!(updates.len(), 1);
    }

    #[test]
    fn test_deshred_filter_account_exclude() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_b = Pubkey::new_unique();

        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![],
                account_exclude: vec![key_b.to_string()],
                account_required: vec![],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        // Transaction with excluded key should not match
        let msg =
            create_message_deshred_transaction(&keypair, vec![key_a, key_b], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message);
        assert!(updates.is_empty());

        // Transaction without excluded key should match
        let msg = create_message_deshred_transaction(&keypair, vec![key_a], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message);
        assert_eq!(updates.len(), 1);
    }

    #[test]
    fn test_deshred_filter_account_required() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_b = Pubkey::new_unique();
        let key_c = Pubkey::new_unique();

        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![key_b.to_string(), key_c.to_string()],
            },
        );

        let config = SubscribeDeshredRequest {
            deshred_transactions,
            ping: None,
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        // Transaction with both required keys should match
        let msg = create_message_deshred_transaction(
            &keypair,
            vec![key_a, key_b],
            vec![key_c],
            vec![],
            false,
        );
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message);
        assert_eq!(updates.len(), 1);

        // Transaction missing one required key should not match
        let msg =
            create_message_deshred_transaction(&keypair, vec![key_a, key_b], vec![], vec![], false);
        let message = Message::DeshredTransaction(msg);
        let updates = filter.get_updates(&message);
        assert!(updates.is_empty());
    }

    #[test]
    fn test_deshred_filter_pong() {
        let config = SubscribeDeshredRequest {
            deshred_transactions: HashMap::new(),
            ping: Some(SubscribeRequestPing { id: 42 }),
        };
        let limit = FilterLimits::default();
        let filter = DeshredFilter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let pong = filter.get_pong_msg();
        assert!(pong.is_some());
        let pong = pong.unwrap();
        assert!(matches!(
            pong.message,
            FilteredUpdateDeshredOneof::Pong(ref p) if p.id == 42
        ));

        // Without ping, no pong
        let config_no_ping = SubscribeDeshredRequest {
            deshred_transactions: HashMap::new(),
            ping: None,
        };
        let filter2 =
            DeshredFilter::new(&config_no_ping, &limit, &mut create_filter_names()).unwrap();
        assert!(filter2.get_pong_msg().is_none());
    }

    #[test]
    fn test_deshred_gate_without_rules_rejects() {
        let keypair = Keypair::new();
        let message = create_message_deshred_transaction(
            &keypair,
            vec![keypair.pubkey()],
            vec![],
            vec![],
            false,
        );

        let gate = DeshredTransactionFilterGate::default();
        assert!(!gate.allows_message(&message));
    }

    #[test]
    fn test_deshred_gate_vote_match_and_miss() {
        let keypair = Keypair::new();
        let pubkey = keypair.pubkey();
        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "votes".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: Some(true),
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let filter = DeshredFilter::new(
            &SubscribeDeshredRequest {
                deshred_transactions,
                ping: None,
            },
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();
        let gate = DeshredTransactionFilterGate::default();
        gate.update_client_rules(1, filter.get_deshred_transaction_filter_rules());

        assert!(gate.allows_message(&create_message_deshred_transaction(
            &keypair,
            vec![pubkey],
            vec![],
            vec![],
            true,
        )));
        assert!(!gate.allows_message(&create_message_deshred_transaction(
            &keypair,
            vec![pubkey],
            vec![],
            vec![],
            false,
        )));
    }

    #[test]
    fn test_deshred_gate_account_include_static_and_loaded() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_static = Pubkey::new_unique();
        let key_loaded = Pubkey::new_unique();
        let mut deshred_transactions = HashMap::new();
        deshred_transactions.insert(
            "f1".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![key_static.to_string(), key_loaded.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );

        let filter = DeshredFilter::new(
            &SubscribeDeshredRequest {
                deshred_transactions,
                ping: None,
            },
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();
        let gate = DeshredTransactionFilterGate::default();
        gate.update_client_rules(1, filter.get_deshred_transaction_filter_rules());

        assert!(gate.allows_message(&create_message_deshred_transaction(
            &keypair,
            vec![key_a, key_static],
            vec![],
            vec![],
            false,
        )));
        assert!(gate.allows_message(&create_message_deshred_transaction(
            &keypair,
            vec![key_a],
            vec![key_loaded],
            vec![],
            false,
        )));
        assert!(!gate.allows_message(&create_message_deshred_transaction(
            &keypair,
            vec![key_a],
            vec![],
            vec![],
            false,
        )));
    }

    #[test]
    fn test_deshred_gate_account_exclude_and_required() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_b = Pubkey::new_unique();
        let key_c = Pubkey::new_unique();
        let mut exclude_filters = HashMap::new();
        exclude_filters.insert(
            "exclude".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![],
                account_exclude: vec![key_b.to_string()],
                account_required: vec![],
            },
        );
        let exclude_filter = DeshredFilter::new(
            &SubscribeDeshredRequest {
                deshred_transactions: exclude_filters,
                ping: None,
            },
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();
        let exclude_gate = DeshredTransactionFilterGate::default();
        exclude_gate.update_client_rules(1, exclude_filter.get_deshred_transaction_filter_rules());

        assert!(
            !exclude_gate.allows_message(&create_message_deshred_transaction(
                &keypair,
                vec![key_a, key_b],
                vec![],
                vec![],
                false,
            ))
        );
        assert!(
            exclude_gate.allows_message(&create_message_deshred_transaction(
                &keypair,
                vec![key_a],
                vec![],
                vec![],
                false,
            ))
        );

        let mut required_filters = HashMap::new();
        required_filters.insert(
            "required".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![key_b.to_string(), key_c.to_string()],
            },
        );
        let required_filter = DeshredFilter::new(
            &SubscribeDeshredRequest {
                deshred_transactions: required_filters,
                ping: None,
            },
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();
        let required_gate = DeshredTransactionFilterGate::default();
        required_gate
            .update_client_rules(1, required_filter.get_deshred_transaction_filter_rules());

        assert!(
            required_gate.allows_message(&create_message_deshred_transaction(
                &keypair,
                vec![key_a, key_b],
                vec![key_c],
                vec![],
                false,
            ))
        );
        assert!(
            !required_gate.allows_message(&create_message_deshred_transaction(
                &keypair,
                vec![key_a, key_b],
                vec![],
                vec![],
                false,
            ))
        );
    }

    #[test]
    fn test_deshred_gate_merges_and_removes_client_rules() {
        let keypair = Keypair::new();
        let key_a = keypair.pubkey();
        let key_b = Pubkey::new_unique();
        let key_c = Pubkey::new_unique();

        let mut filter_one = HashMap::new();
        filter_one.insert(
            "one".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: Some(true),
                account_include: vec![],
                account_exclude: vec![],
                account_required: vec![],
            },
        );
        let filter_one = DeshredFilter::new(
            &SubscribeDeshredRequest {
                deshred_transactions: filter_one,
                ping: None,
            },
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();

        let mut filter_two = HashMap::new();
        filter_two.insert(
            "two".to_string(),
            SubscribeRequestFilterDeshredTransactions {
                vote: None,
                account_include: vec![key_b.to_string()],
                account_exclude: vec![],
                account_required: vec![],
            },
        );
        let filter_two = DeshredFilter::new(
            &SubscribeDeshredRequest {
                deshred_transactions: filter_two,
                ping: None,
            },
            &FilterLimits::default(),
            &mut create_filter_names(),
        )
        .unwrap();

        let gate = DeshredTransactionFilterGate::default();
        gate.update_client_rules(1, filter_one.get_deshred_transaction_filter_rules());
        gate.update_client_rules(2, filter_two.get_deshred_transaction_filter_rules());

        assert!(gate.allows_message(&create_message_deshred_transaction(
            &keypair,
            vec![key_a],
            vec![],
            vec![],
            true,
        )));
        assert!(gate.allows_message(&create_message_deshred_transaction(
            &keypair,
            vec![key_a, key_b],
            vec![],
            vec![],
            false,
        )));

        gate.remove_client(1);
        assert!(!gate.allows_message(&create_message_deshred_transaction(
            &keypair,
            vec![key_a],
            vec![],
            vec![],
            true,
        )));
        assert!(gate.allows_message(&create_message_deshred_transaction(
            &keypair,
            vec![key_a, key_b],
            vec![],
            vec![],
            false,
        )));

        gate.remove_client(2);
        assert!(!gate.allows_message(&create_message_deshred_transaction(
            &keypair,
            vec![key_a, key_b],
            vec![key_c],
            vec![],
            false,
        )));
    }

    #[test]
    fn test_transaction_required_y_z_include_x() {
        let mut transactions = HashMap::new();

        let keypair_x = Keypair::new();
        let account_key_x = keypair_x.pubkey();
        let account_key_y = Pubkey::new_unique();
        let account_key_z = Pubkey::new_unique();

        // require x, include y, z
        let account_include = [account_key_x].iter().map(|k| k.to_string()).collect();
        let account_required = [account_key_y, account_key_z]
            .iter()
            .map(|k| k.to_string())
            .collect();
        transactions.insert(
            "serum".to_string(),
            SubscribeRequestFilterTransactions {
                vote: None,
                failed: None,
                signature: None,
                account_include,
                account_exclude: vec![],
                account_required,
            },
        );

        let config = SubscribeRequest {
            accounts: HashMap::new(),
            slots: HashMap::new(),
            transactions,
            transactions_status: HashMap::new(),
            blocks: HashMap::new(),
            blocks_meta: HashMap::new(),
            entry: HashMap::new(),
            commitment: None,
            accounts_data_slice: Vec::new(),
            ping: None,
            from_slot: None,
        };
        let limit = FilterLimits::default();
        let filter = Filter::new(&config, &limit, &mut create_filter_names()).unwrap();

        let message_transaction =
            create_message_transaction(&keypair_x, vec![account_key_x, account_key_z]);
        let message = Message::Transaction(message_transaction);
        for message in filter.get_updates(&message, None) {
            assert!(message.filters.is_empty());
        }
    }
}
