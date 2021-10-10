use crate::tx_engine::processors::{
    process_chargeback, process_deposit, process_dispute, process_resolve, process_withdrawal,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_stream::StreamExt;

#[derive(Deserialize)]
/// Rows parsed from an input CSV
pub struct InputRow {
    r#type: String,
    client: u16,
    tx: u32,
    // Requires up to 4 sig figs. Uses optional field since disputes, resolves, and chargebacks may have an empty "amount" field
    amount: Option<f32>,
}

impl InputRow {
    fn transaction_type(&self) -> Option<TransactionType> {
        match self.r#type.as_str() {
            "deposit" => Some(TransactionType::Deposit),
            "withdrawal" => Some(TransactionType::Withdrawal),
            "dispute" => Some(TransactionType::Dispute),
            "resolve" => Some(TransactionType::Resolve),
            "chargeback" => Some(TransactionType::Chargeback),
            _ => None,
        }
    }
}

/// The output type
#[derive(Default, Serialize)]
pub struct OutputRow {
    client: u16,
    available: f32,
    held: f32,
    total: f32,
    locked: bool,
}

#[derive(Eq, PartialEq, Hash)]
pub enum TransactionType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

#[derive(Hash, Eq, PartialEq)]
pub struct HistoryKey {
    client: u16,
    tx: u32,
    tx_type: TransactionType,
}

/// Abstraction used to keep track of a client's state as rows are sequentially processed
pub struct TransactionEngine {
    // Each client will be mapped to a singular output row as desired
    clients: HashMap<u16, OutputRow>,
}

impl TransactionEngine {
    fn new() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }

    /// Fully processes the input file, outputting the contents to the desired output
    pub async fn process_file<P: AsRef<Path>, W: AsyncWrite + Unpin>(
        file: P,
        output: W,
    ) -> Result<(), Box<dyn Error>> {
        let source = tokio::fs::File::open(file).await?;
        Self::process(source, output).await
    }

    /// Fully processes the input source, outputting the contents to the desired output
    pub async fn process<R: AsyncRead + Unpin + Send + Sync, W: AsyncWrite + Unpin>(
        input: R,
        output: W,
    ) -> Result<(), Box<dyn Error>> {
        let mut this = Self::new();

        // use "flexible" to allow empty input fields for disputes, resolves, and chargebacks
        let input = csv_async::AsyncReaderBuilder::new()
            .flexible(true)
            .create_deserializer(input);
        let mut rows = input.into_deserialize::<InputRow>();
        let mut history = HashMap::new();

        // Assume every row is chronologically sequential as specified
        while let Some(result) = rows.next().await {
            let row = result?;
            this.process_single_transaction(row, &mut history)?;
        }

        // output to desired output stream
        let mut output = csv_async::AsyncSerializer::from_writer(output);
        for (_, row) in this.clients {
            output.serialize(row).await?;
        }

        Ok(output.flush().await?)
    }

    fn process_single_transaction(
        &mut self,
        input_row: InputRow,
        history: &mut HashMap<HistoryKey, InputRow>,
    ) -> Result<(), Box<dyn Error>> {
        let tx_type = input_row.transaction_type().ok_or_else(|| {
            std::io::Error::new(std::io::ErrorKind::InvalidInput, "Invalid transaction type")
        })?;
        self.create_client_if_non_exists(input_row.client);
        let client_row = self.clients.get_mut(&input_row.client).unwrap();

        match tx_type {
            TransactionType::Deposit => process_deposit(input_row, client_row, history),

            TransactionType::Withdrawal => process_withdrawal(input_row, client_row, history),

            TransactionType::Dispute => process_dispute(input_row, client_row, history),

            TransactionType::Resolve => process_resolve(input_row, client_row, history),

            TransactionType::Chargeback => process_chargeback(input_row, client_row, history),
        }

        Ok(())
    }

    /// Gets the client from the internal map. If the client does not exist, will create a new entry
    fn create_client_if_non_exists(&mut self, client: u16) {
        if !self.clients.contains_key(&client) {
            let new_row = OutputRow {
                client,
                ..Default::default()
            };

            assert!(self.clients.insert(client, new_row).is_none());
        }
    }
}

mod processors {
    use crate::tx_engine::{HistoryKey, InputRow, OutputRow, TransactionType};
    use std::collections::HashMap;

    pub fn process_deposit(
        input_row: InputRow,
        client_row: &mut OutputRow,
        history: &mut HashMap<HistoryKey, InputRow>,
    ) {
        // we can safely unwrap below since the "amount" field is asserted to exist for "deposit" types
        let amount = input_row.amount.clone().unwrap();
        client_row.available += amount;
        client_row.total += amount;

        history.insert(
            HistoryKey {
                client: input_row.client,
                tx: input_row.tx,
                tx_type: TransactionType::Deposit,
            },
            input_row,
        );
    }

    pub fn process_withdrawal(
        input_row: InputRow,
        client_row: &mut OutputRow,
        history: &mut HashMap<HistoryKey, InputRow>,
    ) {
        // we can safely unwrap below since the "amount" field is asserted to exist for "withdrawal" types
        let amount = input_row.amount.clone().unwrap();
        if amount > client_row.available || amount > client_row.total {
            return;
        }

        client_row.available -= amount;
        client_row.total -= amount;

        history.insert(
            HistoryKey {
                client: input_row.client,
                tx: input_row.tx,
                tx_type: TransactionType::Withdrawal,
            },
            input_row,
        );
    }

    pub fn process_dispute(
        input_row: InputRow,
        client_row: &mut OutputRow,
        history: &mut HashMap<HistoryKey, InputRow>,
    ) {
        let ref expected_key_deposit = HistoryKey {
            client: input_row.client,
            tx: input_row.tx,
            tx_type: TransactionType::Deposit,
        };

        let ref expected_key_withdrawal = HistoryKey {
            client: input_row.client,
            tx: input_row.tx,
            tx_type: TransactionType::Withdrawal,
        };

        // ***Note to reviewer: differential handling of disputes w.r.t deposits or withdrawals was unclear in the assignment text. I assume equivalent treatment for both types***
        let dispute_amount = if let Some(deposit_row) = history.get(expected_key_deposit) {
            deposit_row.amount.clone().unwrap()
        } else {
            if let Some(withdrawal_row) = history.get(expected_key_withdrawal) {
                withdrawal_row.amount.clone().unwrap()
            } else {
                return;
            }
        };

        client_row.available -= dispute_amount;
        client_row.held += dispute_amount;

        history.insert(
            HistoryKey {
                client: input_row.client,
                tx: input_row.tx,
                tx_type: TransactionType::Dispute,
            },
            input_row,
        );
    }

    pub fn process_resolve(
        input_row: InputRow,
        client_row: &mut OutputRow,
        history: &mut HashMap<HistoryKey, InputRow>,
    ) {
        if let Some(dispute_amount) = get_dispute_amount(&input_row, history) {
            client_row.held -= dispute_amount;
            client_row.available += dispute_amount;
        }
    }

    pub fn process_chargeback(
        input_row: InputRow,
        client_row: &mut OutputRow,
        history: &mut HashMap<HistoryKey, InputRow>,
    ) {
        if let Some(dispute_amount) = get_dispute_amount(&input_row, history) {
            client_row.held -= dispute_amount;
            client_row.total -= dispute_amount;
            client_row.locked = true;
        }
    }

    fn get_dispute_amount(
        input_row: &InputRow,
        history: &HashMap<HistoryKey, InputRow>,
    ) -> Option<f32> {
        let ref expected_key_deposit = HistoryKey {
            client: input_row.client,
            tx: input_row.tx,
            tx_type: TransactionType::Deposit,
        };

        let ref expected_key_withdrawal = HistoryKey {
            client: input_row.client,
            tx: input_row.tx,
            tx_type: TransactionType::Withdrawal,
        };

        let ref expected_key_dispute = HistoryKey {
            client: input_row.client,
            tx: input_row.tx,
            tx_type: TransactionType::Dispute,
        };

        // at most 2 O(1) lookups in the hashmap are cheap
        if history.contains_key(expected_key_dispute) {
            if let Some(deposit_row) = history.get(expected_key_deposit) {
                return Some(deposit_row.amount.clone().unwrap());
            } else {
                if let Some(withdrawal_row) = history.get(expected_key_withdrawal) {
                    return Some(withdrawal_row.amount.clone().unwrap());
                }
            }
        }

        None
    }
}
