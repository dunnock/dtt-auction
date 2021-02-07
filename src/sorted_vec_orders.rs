use crate::orders::{OrderId, OrderType, RegisteredOrder, RegisteredOrders};
use merging_iterator::MergeIter;
use rayon::slice::ParallelSliceMut;
use slotmap::SparseSecondaryMap;
use std::{
    collections::HashSet,
    ops::{Deref, DerefMut},
};

pub struct SortedOrders {
    order_type: OrderType,
    orders: Vec<RegisteredOrder>,
}

impl SortedOrders {
    pub fn new(order_type: OrderType) -> Self {
        Self {
            order_type,
            orders: Default::default(),
        }
    }

    pub fn add_batch(&mut self, new_orders: &mut Vec<RegisteredOrder>) {
        //let time = Instant::now();
        self.orders.extend_from_slice(&std::mem::take(new_orders));
        if self.order_type == OrderType::Buy {
            self.orders.par_sort_by(|a, b| b.rate.cmp(&a.rate));
        } else {
            self.orders.par_sort_by(|a, b| a.rate.cmp(&b.rate));
        }
        //println!("Merged {:?} orders {} in {} micros", self.order_type, self.orders.len(), time.elapsed().as_micros());
    }

    pub fn add_remove_batch(
        &mut self,
        new_orders: &mut Vec<RegisteredOrder>,
        orders: &RegisteredOrders,
    ) {
        let mut new_orders = std::mem::take(new_orders);
        if self.order_type == OrderType::Buy {
            new_orders.sort_unstable_by(|a, b| b.rate.cmp(&a.rate));
        } else {
            new_orders.sort_unstable_by(|a, b| a.rate.cmp(&b.rate));
        }
        //let time = Instant::now();
        let new_orders = new_orders
            .into_iter()
            .filter(|order| orders.contains_key(order.id));
        let self_orders = std::mem::take(&mut self.orders)
            .into_iter()
            .filter(|order| orders.contains_key(order.id));
        self.orders = if self.order_type == OrderType::Buy {
            MergeIter::with_custom_ordering(new_orders, self_orders, |a, b| b.rate < a.rate)
                .collect()
        } else {
            MergeIter::with_custom_ordering(new_orders, self_orders, |a, b| a.rate < b.rate)
                .collect()
        };
        //println!("Merged {:?} orders {} in {} micros", self.order_type, self.orders.len(), time.elapsed().as_micros());
    }

    pub fn add_remove_hash_set_batch(
        &mut self,
        add: &mut Vec<RegisteredOrder>,
        remove: &mut HashSet<OrderId>,
    ) {
        let remove_set = std::mem::take(remove);
        let mut orders = std::mem::take(add);
        if self.order_type == OrderType::Buy {
            orders.sort_unstable_by(|a, b| b.rate.cmp(&a.rate));
        } else {
            orders.sort_unstable_by(|a, b| a.rate.cmp(&b.rate));
        }
        //let time = Instant::now();
        let orders = orders
            .into_iter()
            .filter(|order| !remove_set.contains(&order.id));
        let self_orders = std::mem::take(&mut self.orders)
            .into_iter()
            .filter(|order| !remove_set.contains(&order.id));
        self.orders = if self.order_type == OrderType::Buy {
            MergeIter::with_custom_ordering(orders, self_orders, |a, b| b.rate < a.rate).collect()
        } else {
            MergeIter::with_custom_ordering(orders, self_orders, |a, b| a.rate < b.rate).collect()
        };
        //println!("Merged {:?} orders {} in {} micros", self.order_type, self.orders.len(), time.elapsed().as_micros());
    }

    pub fn remove_batch(&mut self, orders: &SparseSecondaryMap<OrderId, ()>) {
        self.orders.retain(|order| !orders.contains_key(order.id));
    }
}

impl Deref for SortedOrders {
    type Target = Vec<RegisteredOrder>;
    fn deref(&self) -> &Self::Target {
        &self.orders
    }
}
impl DerefMut for SortedOrders {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.orders
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::orders::Order;
    use nanorand::WyRand;

    #[test]
    fn sorted_orders() {
        for order_type in &[OrderType::Buy, OrderType::Sell] {
            let mut registered = RegisteredOrders::default();
            let mut orders = SortedOrders::new(*order_type);
            let mut rng = WyRand::new_seed(1);
            let mut samples: Vec<_> = (0..)
                .map(|i| (i, Order::random(&mut rng, 100, 1000, 500)))
                .filter(|(_, order)| order.order_type == *order_type)
                .map(|(_, order)| registered.add_get_order(order, 0))
                .take(10_000)
                .collect();
            orders.add_remove_batch(&mut samples, &registered);
            assert_eq!(
                orders.iter().zip(orders.iter().skip(1)).find(|(a, b)| {
                    if *order_type == OrderType::Buy {
                        a.rate < b.rate
                    } else {
                        b.rate < a.rate
                    }
                }),
                None
            );
            assert_eq!(orders.len(), 10_000);
            let mut samples: Vec<_> = (20_000..)
                .map(|i| (i, Order::random(&mut rng, 100, 1000, 500)))
                .filter(|(_, order)| order.order_type == *order_type)
                .map(|(_, order)| registered.add_get_order(order, 1))
                .take(10_000)
                .collect();
            registered.retain(|_, order| order.rate < 900);
            orders.add_remove_batch(&mut samples, &registered);
            for order in orders.iter() {
                assert!(registered.contains_key(order.id));
            }
            assert_eq!(orders.len(), registered.len());
        }
    }
}
