use nanorand::{WyRand, RNG};
use slotmap::HopSlotMap;
use std::ops::{Deref, DerefMut};

pub type Price = i32;
pub type Epoch = u16;

slotmap::new_key_type! {
    pub struct OrderId;
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum OrderType {
    Buy,
    Sell,
}

#[derive(Debug, Clone)]
pub struct Order {
    pub order_type: OrderType,
    pub rate: Price,
    pub quantity: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RegisteredOrder {
    pub id: OrderId,
    pub epoch: Epoch,
    pub order_type: OrderType,
    pub rate: Price,
    pub quantity: u32,
}

#[derive(Default)]
pub struct RegisteredOrders {
    orders: HopSlotMap<OrderId, RegisteredOrder>,
}

impl Order {
    pub fn random(rng: &mut WyRand, prices_min: u32, prices_max: u32, buy_sell_dev: i32) -> Self {
        let buy: bool = rng.generate();
        let price = (rng.generate::<u32>() % (prices_max - prices_min) + prices_min) as i32;
        Self {
            order_type: if buy { OrderType::Buy } else { OrderType::Sell },
            rate: if buy {
                price - buy_sell_dev / 2
            } else {
                price + buy_sell_dev / 2
            },
            quantity: rng.generate_range(1, 1000),
        }
    }
}

impl RegisteredOrder {
    #[inline]
    pub fn init_from_order(id: OrderId, epoch: Epoch, order: Order) -> Self {
        Self {
            id,
            epoch,
            order_type: order.order_type,
            rate: order.rate,
            quantity: order.quantity,
        }
    }
}

impl RegisteredOrders {
    #[inline]
    pub fn remove_order(&mut self, id: OrderId) -> Option<RegisteredOrder> {
        self.orders.remove(id)
    }

    #[inline]
    pub fn get(&mut self, id: OrderId) -> Option<&RegisteredOrder> {
        self.orders.get(id)
    }

    #[inline]
    pub fn add_order(&mut self, order: Order, epoch: Epoch) -> OrderId {
        self.orders
            .insert_with_key(|id| RegisteredOrder::init_from_order(id, epoch, order))
    }

    #[inline]
    pub fn add_get_order(&mut self, order: Order, epoch: Epoch) -> RegisteredOrder {
        let id = self.add_order(order, epoch);
        self[id].clone()
    }

    #[inline]
    pub fn modify_order(&mut self, order: RegisteredOrder) {
        if let Some(original) = self.orders.get_mut(order.id) {
            *original = order;
        }
    }
}

impl Deref for RegisteredOrders {
    type Target = HopSlotMap<OrderId, RegisteredOrder>;
    fn deref(&self) -> &Self::Target {
        &self.orders
    }
}
impl DerefMut for RegisteredOrders {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.orders
    }
}
