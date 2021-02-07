//! Processing pipeline:
//! -- Create Buy/Sell order with id or Cancel order request
//! -- Register order
//! -- Route orders to Buy / Sell collection
//!
//! -- Buy and Sell orders collection: add batch of new orders sorted by price
//!
//! -- Build demand function as aggregated sum of orders price desc
//! --- Requires sorted by price vector of Buy orders
//! -- Build supply function as aggregated sum of orders price asc
//! --- Requires sorted by price vector of Sell orders
//!
//! -- Calculate equilibrium and price / quantity based on sorted collection
//! -- Run trade and provide result:
//! --- Filled orders: Order + quantity + price
//! --- OrderBook for new interval

use std::{
    fmt::{Display, Formatter, Result},
    time::{Duration, Instant},
};

use hft::sorted_vec_orders::SortedOrders;
use hft::{
    market::market_match,
    orders::{Order, OrderId, OrderType, RegisteredOrder, RegisteredOrders},
};
use nanorand::{WyRand, RNG};
use slotmap::SparseSecondaryMap;
use statistical::{mean, standard_deviation};

#[derive(Debug, Clone)]
pub enum OrderRequest {
    CancelOrder(OrderId),
    ModifyOrder(RegisteredOrder),
    AddOrder(Order, u16),
}

#[derive(Default)]
pub struct Stats {
    processing: Vec<Duration>,
    period: Vec<Duration>,
    number_trades: Vec<usize>,
    add_count: Vec<usize>,
    cancel_count: Vec<usize>,
}

const BATCH_SIZE: usize = 10_000;
const ORDERS: usize = 10_000_000;
const EPOCH_NS: u128 = 100_000_000;
const CIRCULATION: usize = 250_000;

fn main() {
    let mut stats = Stats::default();
    let mut orders = RegisteredOrders::default();
    let mut bids = SortedOrders::new(OrderType::Buy);
    let mut asks = SortedOrders::new(OrderType::Sell);
    let mut buy_batch: Vec<RegisteredOrder> = Vec::with_capacity(BATCH_SIZE);
    let mut sell_batch: Vec<RegisteredOrder> = Vec::with_capacity(BATCH_SIZE);
    let mut cancel_ids = SparseSecondaryMap::new();

    println!("Pregenerating input {} orders", ORDERS);
    let mut rng = WyRand::new();
    let input: Vec<_> = (0..ORDERS)
        .map(|_| Order::random(&mut rng, 850_00, 1_150_00, 100_00))
        .enumerate()
        .collect();

    println!("Starting market emulation");
    let total = std::time::Instant::now();
    let mut period = std::time::Instant::now();
    let mut epoch = 0;
    let mut cancel_is_bid = true;
    let mut cancel_count = 0;
    let mut add_count = 0;

    for (i, order) in input {
        let cancel = if orders.len() < CIRCULATION {
            false
        } else {
            // 50% chance to cancel orders after CIRCULATION boundary reached
            rng.generate::<u8>() < 128
        };

        // 1. Generate request
        let request = if cancel {
            if let Some(order) = if cancel_is_bid {
                bids.pop()
            } else {
                asks.pop()
            } {
                cancel_is_bid = !cancel_is_bid;
                cancel_count += 1;
                OrderRequest::CancelOrder(order.id)
            } else {
                continue;
            }
        } else {
            add_count += 1;
            OrderRequest::AddOrder(order, epoch)
        };

        // 2. Process request
        let registered = match request.clone() {
            OrderRequest::CancelOrder(id) => orders.remove_order(id),
            OrderRequest::AddOrder(order, epoch) => Some(orders.add_get_order(order, epoch)),
            _ => unimplemented!("modify orders not supported"),
        }
        .unwrap_or_else(|| panic!("Mismatching order {} {:?}", i, request));

        // 3. Add to batch for processing
        match (registered.order_type, request) {
            (_, OrderRequest::CancelOrder(id)) => {
                cancel_ids.insert(id, ());
            }
            (OrderType::Buy, OrderRequest::AddOrder(..)) => {
                buy_batch.push(registered);
            }
            (OrderType::Sell, OrderRequest::AddOrder(..)) => {
                sell_batch.push(registered);
            }
            _ => {}
        }

        // 4. Submit batch on condition
        if buy_batch.len() + sell_batch.len() >= BATCH_SIZE
            && period.elapsed().as_nanos() < EPOCH_NS
        {
            rayon::join(
                || {
                    bids.add_batch(&mut buy_batch);
                    bids.remove_batch(&cancel_ids);
                },
                || {
                    asks.add_batch(&mut sell_batch);
                    asks.remove_batch(&cancel_ids);
                },
            );
            cancel_ids.clear();
        } else
        // Process market every EPOCH_NS nanos
        if period.elapsed().as_nanos() >= EPOCH_NS {
            let processing_t = Instant::now();
            println!("## Processing auction. Total {} open orders after clearing {} cancel orders.", orders.len(), cancel_count);

            rayon::join(
                || {
                    bids.add_batch(&mut buy_batch);
                    bids.remove_batch(&cancel_ids);
                },
                || {
                    asks.add_batch(&mut sell_batch);
                    asks.remove_batch(&cancel_ids);
                },
            );
            cancel_ids.clear();
            println!(
                "Finished final sorting in {} µs",
                processing_t.elapsed().as_micros()
            );
            // 5. Market equilibrium

            let match_result = market_match(
                std::mem::replace(&mut bids, SortedOrders::new(OrderType::Buy)),
                std::mem::replace(&mut asks, SortedOrders::new(OrderType::Sell)),
            );

            println!(
                "Matched {} buy orders with {} sell orders with total volume {} on price {:?}.",
                match_result.bids_matched,
                match_result.asks_matched,
                match_result.traded_volume,
                match_result.traded_rate,
            );
            println!("Cleared {} orders", match_result.trades.len());
            println!(
                "Stays open {} buy orders and {} sell orders",
                match_result.open_bids.len(),
                match_result.open_asks.len(),
            );

            bids = match_result.open_bids;
            asks = match_result.open_asks;

            stats.add_period(
                processing_t.elapsed(),
                period.elapsed(),
                match_result.trades.len(),
                add_count,
                cancel_count,
            );
            println!("## Period Summary");
            println!(
                "Auction processed in {} ms",
                processing_t.elapsed().as_millis()
            );
            println!("Period completed in {} ms", period.elapsed().as_millis());

            period = Instant::now();
            epoch += 1;
            cancel_count = 0;
            add_count = 0;
            // Clear all orders processed in previous auction
            for deal in match_result.trades.iter() {
                if deal.quantity == deal.order.quantity {
                    orders.remove_order(deal.order.id);
                } else {
                    let mut order = deal.order.clone();
                    order.quantity = deal.quantity;
                    orders.modify_order(order);
                }
            }
            println!(
                "\n \
                Starting epoch {} with {} open orders.\n \
                Current input order N {}",
                epoch,
                orders.len(),
                i
            );
        }
    }

    println!(
        "Processed {} orders in {}s.",
        ORDERS,
        total.elapsed().as_secs()
    );
    println!("\n## Processing summary:\n{}", stats);
}

impl Stats {
    pub fn add_period(&mut self, processing: Duration, period: Duration, trades: usize, add_count: usize, cancel_count: usize) {
        self.processing.push(processing);
        self.period.push(period);
        self.number_trades.push(trades);
        self.add_count.push(add_count);
        self.cancel_count.push(cancel_count);
    }
}

impl Display for Stats {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let processing: Vec<_> = self
            .processing
            .iter()
            .map(|p| p.as_micros() as f64 / 1000.0)
            .collect();
        let periods: Vec<_> = self
            .period
            .iter()
            .map(|p| p.as_micros() as f64 / 1000.0)
            .collect();
        let trades: Vec<_> = self.number_trades.iter().map(|t| *t as f64).collect();
        let adds: Vec<_> = self.add_count.iter().map(|t| *t as f64).collect();
        let cancels: Vec<_> = self.cancel_count.iter().map(|t| *t as f64).collect();
        write!(
            f,
            "Processing time: mean {:.3}ms dev {:.3}\n",
            mean(&processing),
            standard_deviation(&processing, None)
        )?;
        write!(
            f,
            "Period time including processing: mean {:.3}ms dev {:.3}\n",
            mean(&periods),
            standard_deviation(&periods, None)
        )?;
        write!(
            f,
            "Number of trades per period: mean {:.1} dev {:.1}\n",
            mean(&trades),
            standard_deviation(&trades, None)
        )?;
        write!(
            f,
            "Number of add orders per period: mean {:.1} dev {:.1}\n",
            mean(&adds),
            standard_deviation(&adds, None)
        )?;
        write!(
            f,
            "Number of cancelled orders per period: mean {:.1} dev {:.1}\n",
            mean(&cancels),
            standard_deviation(&cancels, None)
        )
    }
}
