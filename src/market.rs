use std::time::Instant;

use crate::{
    orders::{OrderType, Price, RegisteredOrder},
    sorted_vec_orders::SortedOrders,
};
use merging_iterator::MergeIter;

#[derive(Debug)]
pub struct Trade {
    pub order: RegisteredOrder,
    pub rate: Price,
    pub quantity: u32,
}

pub struct MarketMatchResult {
    pub open_bids: SortedOrders,
    pub open_asks: SortedOrders,
    pub trades: Vec<Trade>,
    pub traded_volume: u64,
    pub traded_rate: Option<Price>,
    pub bids_matched: usize,
    pub asks_matched: usize,
}

impl MarketMatchResult {
    fn no_trade(open_bids: SortedOrders, open_asks: SortedOrders) -> Self {
        Self {
            open_bids,
            open_asks,
            trades: Default::default(),
            traded_volume: 0,
            traded_rate: None,
            bids_matched: 0,
            asks_matched: 0,
        }
    }
}

pub fn market_match(mut bids: SortedOrders, mut asks: SortedOrders) -> MarketMatchResult {
    let time1 = Instant::now();
    let bids_iter = bids.iter().map(aggregate_quantity());
    let asks_iter = asks.iter().map(aggregate_quantity());
    // Merged iterator of buy vs sell over balanced quantity
    let balanced_orders = MergeIter::with_custom_ordering(
        bids_iter,
        asks_iter,
        |(_, bid_volume), (_, ask_volume)| bid_volume < ask_volume,
    );
    // Walk through demand and supply while prices fit
    let mut bid = Price::MAX;
    let mut ask = Price::MIN;
    let mut bid_idx: usize = 0;
    let mut ask_idx: usize = 0;
    let mut bid_volume: u64 = 0;
    let mut ask_volume: u64 = 0;
    let total_matched = balanced_orders
        .take_while(|(order, volume)| match order.order_type {
            OrderType::Buy if order.rate >= ask => {
                bid_volume = *volume;
                bid_idx += 1;
                bid = order.rate;
                true
            }
            OrderType::Sell if bid >= order.rate => {
                ask_volume = *volume;
                ask = order.rate;
                ask_idx += 1;
                true
            }
            _ => false,
        })
        .count();
    bid_idx -= 1;
    ask_idx -= 1;
    println!(
        "Approximate equilibrium at bid[{}] ask[{}] in {}micros",
        bid_idx,
        ask_idx,
        time1.elapsed().as_micros()
    );

    if total_matched < 2 {
        return MarketMatchResult::no_trade(bids, asks);
    }

    let time2 = Instant::now();
    // We just need
    let mut deals = Vec::new();
    let mut bid_orders_matched = 0;
    let mut ask_orders_matched = 0;
    let traded_volume;
    let (mut bid, mut ask) = (&bids[bid_idx], &asks[ask_idx]);
    // Market rate
    let rate = (bid.rate + ask.rate) / 2;
    let traded_rate = Some(rate);
    // One order might be only partially filled if resulting
    // demand / supply quanity does not match
    if bid_volume > ask_volume {
        // Go down on buy orders until we find one which would match sell volume
        while bid_volume - bid.quantity as u64 > ask_volume {
            bid_volume -= bid.quantity as u64;
            bid_idx -= 1;
            bid = &bids[bid_idx];
        }
        traded_volume = ask_volume;
        deals.push(Trade {
            quantity: (bid.quantity as u64 + ask_volume - bid_volume) as u32,
            rate,
            order: bid.clone(),
        });
        bid_idx -= 1;
        bid_orders_matched += 1;
    } else if bid_volume < ask_volume {
        // Go down on sell orders until we find one which would match buy volume
        while ask_volume - ask.quantity as u64 > bid_volume {
            ask_volume -= ask.quantity as u64;
            ask_idx -= 1;
            ask = &asks[ask_idx];
        }
        traded_volume = bid_volume;
        deals.push(Trade {
            quantity: (ask.quantity as u64 + bid_volume - ask_volume) as u32,
            rate,
            order: ask.clone(),
        });
        ask_idx -= 1;
        ask_orders_matched += 1;
    } else {
        traded_volume = bid_volume;
    };

    bid_orders_matched += bid_idx + 1;
    ask_orders_matched += ask_idx + 1;

    deals.extend(
        bids.drain(0..=bid_idx)
            .chain(asks.drain(0..=ask_idx))
            .map(|order| Trade {
                rate,
                quantity: order.quantity,
                order,
            }),
    );
    println!(
        "Built market results in {} micros",
        time2.elapsed().as_micros()
    );
    MarketMatchResult {
        open_bids: bids,
        open_asks: asks,
        trades: deals,
        traded_volume,
        traded_rate,
        bids_matched: bid_orders_matched,
        asks_matched: ask_orders_matched,
    }
}

#[inline]
fn aggregate_quantity() -> impl FnMut(&RegisteredOrder) -> (&RegisteredOrder, u64) {
    let mut quantity: u64 = 0;
    move |order: &RegisteredOrder| {
        quantity += order.quantity as u64;
        (order, quantity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orders::{Order, RegisteredOrder, RegisteredOrders};

    fn test_order(
        registered: &mut RegisteredOrders,
        rate: Price,
        quantity: u32,
        order_type: OrderType,
    ) -> RegisteredOrder {
        let order = Order {
            order_type,
            quantity,
            rate,
        };
        registered.add_get_order(order, 0)
    }

    fn test_data(buy_quantity: u32, sell_quantity: u32) -> (SortedOrders, SortedOrders) {
        let mut orders = RegisteredOrders::default();
        let mut buy_samples: Vec<_> = (1..=100)
            .map(|i| test_order(&mut orders, i, buy_quantity, OrderType::Buy))
            .collect();
        let mut sell_samples: Vec<_> = (1..=100)
            .map(|i| test_order(&mut orders, i, sell_quantity, OrderType::Sell))
            .collect();
        let mut buy_orders = SortedOrders::new(OrderType::Buy);
        let mut sell_orders = SortedOrders::new(OrderType::Sell);
        buy_orders.add_remove_batch(&mut buy_samples, &orders);
        sell_orders.add_remove_batch(&mut sell_samples, &orders);
        (buy_orders, sell_orders)
    }

    #[test]
    fn market_match_result_quantity_1() {
        let (bid_orders, ask_orders) = test_data(1, 1);
        assert_eq!(bid_orders.first().unwrap().rate, 100);
        assert_eq!(ask_orders.first().unwrap().rate, 1);

        let result = market_match(bid_orders, ask_orders);
        assert_eq!(result.traded_rate, Some(51));
        assert_eq!(result.traded_volume, 50);

        let (bids, asks): (Vec<_>, Vec<_>) = result
            .trades
            .iter()
            .partition(|deal| deal.order.order_type == OrderType::Buy);
        assert!(bids
            .iter()
            .all(|deal| Some(deal.rate) <= result.traded_rate));
        assert!(asks
            .iter()
            .all(|deal| Some(deal.rate) >= result.traded_rate));
        assert_eq!(
            bids.iter().map(|deal| deal.quantity).sum::<u32>(),
            asks.iter().map(|deal| deal.quantity).sum::<u32>()
        );
    }

    #[test]
    fn market_match_result_big_quantity_buy_side() {
        let (bid_orders, ask_orders) = test_data(10, 1);

        let result = market_match(bid_orders, ask_orders);
        assert_eq!(result.traded_rate, Some(92));
        assert_eq!(result.traded_volume, 90);
        let (bids, asks): (Vec<_>, Vec<_>) = result
            .trades
            .iter()
            .partition(|deal| deal.order.order_type == OrderType::Buy);
        assert!(bids
            .iter()
            .all(|deal| Some(deal.rate) <= result.traded_rate));
        assert!(asks
            .iter()
            .all(|deal| Some(deal.rate) >= result.traded_rate));
        assert_eq!(
            bids.iter().map(|deal| deal.quantity).sum::<u32>(),
            asks.iter().map(|deal| deal.quantity).sum::<u32>()
        );
    }

    #[test]
    fn market_match_result_big_quantity_sell_side() {
        let (bid_orders, ask_orders) = test_data(1, 10);

        let result = market_match(bid_orders, ask_orders);
        assert_eq!(result.traded_rate, Some(9));
        assert_eq!(result.traded_volume, 90);

        let (bids, asks): (Vec<_>, Vec<_>) = result
            .trades
            .iter()
            .partition(|deal| deal.order.order_type == OrderType::Buy);
        assert!(bids
            .iter()
            .all(|deal| Some(deal.rate) <= result.traded_rate));
        assert!(asks
            .iter()
            .all(|deal| Some(deal.rate) >= result.traded_rate));
        assert_eq!(
            bids.iter().map(|deal| deal.quantity).sum::<u32>(),
            asks.iter().map(|deal| deal.quantity).sum::<u32>()
        );
    }
}
