//! TPC-DS data maintenance: the refresh data sets of `dsdgen -update <n>`.
//!
//! A refresh data set (TPC-DS specification, clause 5.2) is the flat files
//! the data maintenance functions load: three sales channels, each an order
//! file, a line item file and a returns file, plus the inventory file and the
//! two files with the date ranges to delete. [`UpdateSet`] writes them
//! byte-for-byte as the C `dsdgen` (v4.0.0) does:
//!
//! | File | Writer |
//! |---|---|
//! | `s_purchase_<n>.dat`, `s_purchase_lineitem_<n>.dat`, `s_store_returns_<n>.dat` | [`UpdateSet::write_store`] |
//! | `s_catalog_order_<n>.dat`, `s_catalog_order_lineitem_<n>.dat`, `s_catalog_returns_<n>.dat` | [`UpdateSet::write_catalog`] |
//! | `s_web_order_<n>.dat`, `s_web_order_lineitem_<n>.dat`, `s_web_returns_<n>.dat` | [`UpdateSet::write_web`] |
//! | `s_inventory_<n>.dat` | [`UpdateSet::write_inventory`] |
//! | `delete_<n>.dat`, `inventory_delete_<n>.dat` | [`UpdateSet::write_delete`], [`UpdateSet::write_inventory_delete`] |
//!
//! `dsdgen -update` also writes the source tables that TPC-DS v1 used to
//! update the dimensions (`s_customer`, `s_item`, `s_zip_to_gmt`, ...); they
//! are not part of the refresh data since v2 and are not generated here.
//!
//! # dsdgen's behaviour, kept as is
//!
//! - Every update set is generated from the same random number streams; only
//!   the dates (and so the row counts, and the line items shipped after
//!   "today" that cannot be returned) differ between sets.
//! - Order numbers start at 1 in every set: `getUpdateBase()` reads the next
//!   insert value of the wrong table, which is always 0.
//! - The returns copy the prices of their line item (`set_pricing()` has no
//!   case for the returns' pricing columns), and fee, refunded cash, reversed
//!   charge and store credit are an unset decimal, printed as `0`.
//! - The purchase, catalog and web line items pick their item from one
//!   counter: catalog orders start it at -1, purchases continue it, each web
//!   order restarts it at a random item. The first catalog line item reads the
//!   entry before the item permutation (item 1). A web order near the end of
//!   the items reads past the permutation; at scale factors with more than
//!   about 33,000 items the permutation is a fresh memory mapping and those
//!   entries read 0 (item 1), which is what is generated here.

use crate::business_key_generator::make_business_key;
use crate::config::{Scaling, Table};
use crate::distribution::calendar_distribution::{CalendarDistribution, CalendarWeights};
use crate::distribution::hours_distribution::HoursWeights;
use crate::join_key_utils::{
    generate_catalog_page_join_key, generate_scd_join_key, generate_time_join_key,
    generate_uniform_join_key,
};
use crate::permutations::make_permutation;
use crate::random::{RandomNumberStream, RandomTextGenerator, RandomValueGenerator};
use crate::types::pricing::{generate_pricing_for_sales_table, get_web_sales_pricing_limits};
use crate::types::{Date, Pricing};
use std::io::{self, Write};

// Columns (columns.h) of the source tables whose random number streams the
// generators draw from.
const S_CORD_BILL_CUSTOMER_ID: i32 = 518;
const S_CORD_SHIP_CUSTOMER_ID: i32 = 519;
const S_CORD_ORDER_TIME: i32 = 521;
const S_CORD_SHIP_MODE_ID: i32 = 522;
const S_CLIN_ITEM_ID: i32 = 524;
const S_CORD_COMMENT: i32 = 525;
const S_CLIN_PROMOTION_ID: i32 = 528;
const S_CLIN_WAREHOUSE_ID: i32 = 531;
const S_CLIN_SHIP_DATE: i32 = 532;
const S_CLIN_CATALOG_PAGE_ID: i32 = 534;
const S_CLIN_PRICING: i32 = 535;
const S_CLIN_IS_RETURNED: i32 = 537;
const S_CLIN_PERMUTE: i32 = 538;
const S_CRET_CALL_CENTER_ID: i32 = 551;
const S_CRET_DATE: i32 = 557;
const S_CRET_TIME: i32 = 558;
const S_CRET_REASON_ID: i32 = 567;
const S_CRET_PRICING: i32 = 568;
const S_CRET_SHIPMODE_ID: i32 = 569;
const S_CRET_WAREHOUSE_ID: i32 = 570;
const S_INVN_QUANTITY: i32 = 624;
const S_PURCHASE_STORE_ID: i32 = 671;
const S_PURCHASE_CUSTOMER_ID: i32 = 672;
const S_PURCHASE_REGISTER: i32 = 675;
const S_PURCHASE_CLERK: i32 = 676;
const S_PURCHASE_COMMENT: i32 = 677;
const S_PLINE_ITEM_ID: i32 = 679;
const S_PLINE_PROMOTION_ID: i32 = 682;
const S_PLINE_QUANTITY: i32 = 684;
const S_PLINE_COMMENT: i32 = 686;
const S_PLINE_PRICING: i32 = 687;
const S_PLINE_IS_RETURNED: i32 = 688;
const S_PLINE_PERMUTE: i32 = 689;
const S_SRET_RETURN_DATE: i32 = 715;
const S_SRET_RETURN_TIME: i32 = 716;
const S_SRET_REASON_ID: i32 = 726;
const S_SRET_PRICING: i32 = 727;
const S_WORD_BILL_CUSTOMER_ID: i32 = 741;
const S_WORD_SHIP_CUSTOMER_ID: i32 = 742;
const S_WORD_ORDER_TIME: i32 = 744;
const S_WORD_SHIP_MODE_ID: i32 = 745;
const S_WORD_WEB_SITE_ID: i32 = 746;
const S_WORD_COMMENT: i32 = 747;
const S_WLIN_ITEM_ID: i32 = 748;
const S_WLIN_PROMOTION_ID: i32 = 751;
const S_WLIN_WAREHOUSE_ID: i32 = 754;
const S_WLIN_SHIP_DATE: i32 = 755;
const S_WLIN_WEB_PAGE_ID: i32 = 756;
const S_WLIN_PRICING: i32 = 757;
const S_WLIN_IS_RETURNED: i32 = 759;
const S_WLIN_PERMUTE: i32 = 760;
const S_WRET_RETURN_DATE: i32 = 784;
const S_WRET_RETURN_TIME: i32 = 785;
const S_WRET_REASON_ID: i32 = 786;
const S_WRET_PRICING: i32 = 787;

/// Seeds each column's stream gives a row (streams.h), from the column at the
/// start of each range. A row is an order, with its line items and returns.
const S_CATALOG_ORDER_SEEDS: (i32, &[u32]) = (
    517,
    &[
        0, 1, 2, 1, 1, 1, 1, 1, 100, 1, 0, 1, 1, 1, 1, 1, 1, 2, 8, 0, 1, 0,
    ],
);
const S_CATALOG_RETURNS_SEEDS: (i32, &[u32]) = (
    551,
    &[
        9, 0, 0, 0, 0, 0, 9, 18, 0, 0, 0, 0, 0, 0, 0, 0, 9, 72, 9, 9, 0,
    ],
);
const S_PURCHASE_SEEDS: (i32, &[u32]) = (
    670,
    &[
        0, 1, 1, 1, 1, 1, 1, 100, 7, 1, 12, 12, 12, 12, 12, 12, 1200, 96, 12, 0,
    ],
);
const S_STORE_RETURNS_SEEDS: (i32, &[u32]) = (
    710,
    &[0, 0, 0, 0, 0, 24, 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 12, 84],
);
const S_WEB_ORDER_SEEDS: (i32, &[u32]) = (
    740,
    &[
        1, 1, 2, 1, 1, 1, 1, 100, 1, 12, 0, 12, 12, 12, 12, 12, 12, 96, 0, 12, 0,
    ],
);
const S_WEB_RETURNS_SEEDS: (i32, &[u32]) = (778, &[0, 0, 0, 0, 0, 0, 24, 12, 12, 84]);

/// Length of the order comments (`RS_S_PURCHASE_COMMENT`, ...); they are at
/// least 60% of it.
const COMMENT_LENGTH: i32 = 100;
/// Percentage of purchase and web line items that are returned (`WR_RETURN_PCT`)
const RETURN_PCT: i32 = 10;
/// Catalog line items returned, per 10,000 (`S_CATALOG_RETURN_PCT`)
const CATALOG_RETURN_PER_10000: i32 = 1030;
/// Percentage of catalog and web orders shipped to the ordering customer
/// (`WS_GIFT_PCT`)
const GIFT_PCT: i32 = 7;
/// Line items per order
const PURCHASE_LINES: i64 = 12;
const CATALOG_LINES: i64 = 9;
const WEB_LINES: i64 = 12;
/// Catalogs per year (`CP_CATALOGS_PER_YEAR`)
const CATALOGS_PER_YEAR: u64 = 18;
/// Days in the three calendar zones (low, medium and high sales) of an update set
const UPDATE_DATES: usize = 6;

/// The random number streams of a range of columns, padded to their seeds
/// per row at the end of each row (dsdgen's `row_stop()`).
struct Streams {
    first_column: i32,
    streams: Vec<RandomNumberStream>,
}

impl Streams {
    fn new((first_column, seeds_per_row): (i32, &[u32])) -> Self {
        let streams = seeds_per_row
            .iter()
            .zip(first_column..)
            .map(|(&seeds, column)| RandomNumberStream::new_with_column(column, seeds))
            .collect();
        Self {
            first_column,
            streams,
        }
    }

    fn get(&mut self, column: i32) -> &mut RandomNumberStream {
        &mut self.streams[(column - self.first_column) as usize]
    }

    fn row_stop(&mut self) {
        for stream in &mut self.streams {
            while stream.get_seeds_used() < stream.get_seeds_per_row() {
                stream.next_random();
            }
            stream.reset_seeds_used();
        }
    }
}

/// One refresh data set: the data `dsdgen -scale <sf> -update <number>` writes.
#[derive(Debug, Clone)]
pub struct UpdateSet {
    scaling: Scaling,
    /// Sales dates, two adjacent days in each calendar zone
    dates: [i32; UPDATE_DATES],
    /// Inventory dates, two adjacent Thursdays in each calendar zone
    inventory_dates: [i32; UPDATE_DATES],
    /// Orders up to and including each sales date, per channel
    store_orders: [i64; UPDATE_DATES],
    catalog_orders: [i64; UPDATE_DATES],
    web_orders: [i64; UPDATE_DATES],
    /// Items, by ID, and warehouses
    item_count: i64,
    warehouse_count: i64,
}

impl UpdateSet {
    /// The update set `number` (1, 2, ...) for the data at `scale_factor`.
    pub fn new(scale_factor: f64, number: i32) -> Self {
        assert!(number >= 1, "update sets are numbered from 1, got {number}");
        let scaling = Scaling::new(scale_factor);
        let (dates, inventory_dates) = update_dates(number);
        let orders = |table: Table| {
            let mut orders = [0; UPDATE_DATES];
            let mut total = 0;
            for (orders, &date) in orders.iter_mut().zip(&dates) {
                total += scaling.get_row_count_for_date(table, i64::from(date)) as i64;
                *orders = total;
            }
            orders
        };
        Self {
            store_orders: orders(Table::StoreSales),
            catalog_orders: orders(Table::CatalogSales),
            web_orders: orders(Table::WebSales),
            item_count: scaling.get_id_count(Table::Item) as i64,
            warehouse_count: scaling.get_row_count(Table::Warehouse) as i64,
            scaling,
            dates,
            inventory_dates,
        }
    }

    /// Number of purchases (`s_purchase` rows)
    pub fn purchase_count(&self) -> i64 {
        self.store_orders[UPDATE_DATES - 1]
    }

    /// Number of catalog orders (`s_catalog_order` rows)
    pub fn catalog_order_count(&self) -> i64 {
        self.catalog_orders[UPDATE_DATES - 1]
    }

    /// Number of web orders (`s_web_order` rows)
    pub fn web_order_count(&self) -> i64 {
        self.web_orders[UPDATE_DATES - 1]
    }

    /// Number of `s_inventory` rows: every item in every warehouse, on each
    /// of the six inventory dates
    pub fn inventory_count(&self) -> i64 {
        self.item_count * self.warehouse_count * UPDATE_DATES as i64
    }

    /// `delete_<n>.dat`: the three date ranges of sales (and their returns)
    /// to delete
    pub fn write_delete<W: Write>(&self, out: &mut W) -> io::Result<()> {
        write_date_pairs(out, &self.dates)
    }

    /// `inventory_delete_<n>.dat`: the three date ranges of inventory to delete
    pub fn write_inventory_delete<W: Write>(&self, out: &mut W) -> io::Result<()> {
        write_date_pairs(out, &self.inventory_dates)
    }

    /// `s_purchase_<n>.dat`, `s_purchase_lineitem_<n>.dat` and
    /// `s_store_returns_<n>.dat`
    pub fn write_store<P: Write, L: Write, R: Write>(
        &self,
        purchases: &mut P,
        lineitems: &mut L,
        returns: &mut R,
    ) -> io::Result<()> {
        let scaling = &self.scaling;
        let text = RandomTextGenerator::new();
        let mut streams = Streams::new(S_PURCHASE_SEEDS);
        let mut return_streams = Streams::new(S_STORE_RETURNS_SEEDS);
        let permutation = make_permutation(self.item_count as usize, streams.get(S_PLINE_PERMUTE));
        // the purchases continue the item counter where the catalog orders left it
        let mut item_index = advance_item_index(
            -1,
            self.catalog_order_count() * CATALOG_LINES,
            self.item_count,
        );
        let mut line = Vec::new();

        for purchase in 1..=self.purchase_count() {
            let store =
                generate_scd_join_key(Table::Store, streams.get(S_PURCHASE_STORE_ID), 1, scaling);
            let customer = generate_uniform_join_key(
                Table::Customer,
                streams.get(S_PURCHASE_CUSTOMER_ID),
                scaling,
            );
            let date = update_date(&self.store_orders, &self.dates, purchase);
            let register = uniform_int(1, 17, streams.get(S_PURCHASE_REGISTER));
            let clerk = uniform_int(101, 300, streams.get(S_PURCHASE_CLERK));
            let comment = order_comment(&text, streams.get(S_PURCHASE_COMMENT));
            // dsdgen draws a start item into a variable nothing reads
            uniform_int(1, self.item_count as i32, streams.get(S_PLINE_ITEM_ID));

            for number in 1..=PURCHASE_LINES {
                item_index = advance_item_index(item_index, 1, self.item_count);
                let item = first_surrogate_key(permutation_entry(&permutation, item_index));
                let promotion = generate_uniform_join_key(
                    Table::Promotion,
                    streams.get(S_PLINE_PROMOTION_ID),
                    scaling,
                );
                // drawn, then replaced by the quantity the pricing draws
                uniform_int(1, 100, streams.get(S_PLINE_QUANTITY));
                let pricing = sales_pricing(streams.get(S_PLINE_PRICING));
                let comment = text.random_text(1, COMMENT_LENGTH, streams.get(S_PLINE_COMMENT));

                line.clear();
                write!(line, "{purchase}|{number}|")?;
                push_id(&mut line, item);
                push_id(&mut line, promotion);
                writeln!(
                    line,
                    "{}|{}|{}|{comment}|",
                    pricing.get_quantity(),
                    pricing.get_sales_price(),
                    pricing.get_coupon_amount()
                )?;
                lineitems.write_all(&line)?;

                if uniform_int(0, 99, streams.get(S_PLINE_IS_RETURNED)) < RETURN_PCT {
                    let lag = uniform_int(0, 60, return_streams.get(S_SRET_RETURN_DATE));
                    let time = generate_time_join_key(
                        HoursWeights::Uniform,
                        return_streams.get(S_SRET_RETURN_TIME),
                    );
                    let reason = generate_uniform_join_key(
                        Table::Reason,
                        return_streams.get(S_SRET_REASON_ID),
                        scaling,
                    );
                    let quantity = uniform_int(
                        1,
                        pricing.get_quantity(),
                        return_streams.get(S_SRET_PRICING),
                    );

                    line.clear();
                    push_id(&mut line, store);
                    write!(line, "{purchase}|{number}|")?;
                    push_id(&mut line, item);
                    push_id(&mut line, customer);
                    push_date(&mut line, date + lag);
                    push_time(&mut line, time);
                    write!(
                        line,
                        "{purchase}|{quantity}|{}|{}|0|{}|0|0|0|",
                        pricing.get_sales_price(),
                        pricing.get_ext_tax(),
                        pricing.get_ext_ship_cost()
                    )?;
                    push_id(&mut line, reason);
                    line.push(b'\n');
                    returns.write_all(&line)?;
                }
            }

            line.clear();
            write!(line, "{purchase}|")?;
            push_id(&mut line, store);
            push_id(&mut line, customer);
            push_date(&mut line, date);
            writeln!(line, "0|{register}|{clerk}|{comment}|")?;
            purchases.write_all(&line)?;

            streams.row_stop();
            return_streams.row_stop();
        }
        Ok(())
    }

    /// `s_catalog_order_<n>.dat`, `s_catalog_order_lineitem_<n>.dat` and
    /// `s_catalog_returns_<n>.dat`
    pub fn write_catalog<O: Write, L: Write, R: Write>(
        &self,
        orders: &mut O,
        lineitems: &mut L,
        returns: &mut R,
    ) -> io::Result<()> {
        let scaling = &self.scaling;
        let text = RandomTextGenerator::new();
        let mut streams = Streams::new(S_CATALOG_ORDER_SEEDS);
        let mut return_streams = Streams::new(S_CATALOG_RETURNS_SEEDS);
        let permutation = make_permutation(self.item_count as usize, streams.get(S_CLIN_PERMUTE));
        let pages_per_catalog = (scaling.get_row_count(Table::CatalogPage)
            / CATALOGS_PER_YEAR
            / (Date::DATE_MAXIMUM.year() - Date::DATE_MINIMUM.year() + 2) as u64)
            as i64;
        let mut item_index = -1;
        let mut line = Vec::new();

        for order in 1..=self.catalog_order_count() {
            let date = update_date(&self.catalog_orders, &self.dates, order);
            let time = uniform_int(0, 24 * 3600 - 1, streams.get(S_CORD_ORDER_TIME));
            let (bill_customer, ship_customer) = customers(
                scaling,
                &mut streams,
                S_CORD_BILL_CUSTOMER_ID,
                S_CORD_SHIP_CUSTOMER_ID,
            );
            let ship_mode = generate_uniform_join_key(
                Table::ShipMode,
                streams.get(S_CORD_SHIP_MODE_ID),
                scaling,
            );
            let comment = order_comment(&text, streams.get(S_CORD_COMMENT));
            uniform_int(1, self.item_count as i32, streams.get(S_CLIN_ITEM_ID));

            for number in 1..=CATALOG_LINES {
                item_index = advance_item_index(item_index, 1, self.item_count);
                let item = first_surrogate_key(permutation_entry(&permutation, item_index));
                let promotion = generate_uniform_join_key(
                    Table::Promotion,
                    streams.get(S_CLIN_PROMOTION_ID),
                    scaling,
                );
                let warehouse = generate_uniform_join_key(
                    Table::Warehouse,
                    streams.get(S_CLIN_WAREHOUSE_ID),
                    scaling,
                );
                // an order ships after it is made, and not after "today"
                let ship_date = date + uniform_int(0, 60, streams.get(S_CLIN_SHIP_DATE));
                let shipped = ship_date <= Date::JULIAN_TODAYS_DATE;
                let page = generate_catalog_page_join_key(
                    streams.get(S_CLIN_CATALOG_PAGE_ID),
                    i64::from(date),
                    scaling,
                );
                let pricing = sales_pricing(streams.get(S_CLIN_PRICING));

                line.clear();
                write!(line, "{order}|{number}|")?;
                push_id(&mut line, item);
                push_id(&mut line, promotion);
                write!(
                    line,
                    "{}|{}|{}|",
                    pricing.get_quantity(),
                    pricing.get_sales_price(),
                    pricing.get_coupon_amount()
                )?;
                push_id(&mut line, warehouse);
                if shipped {
                    push_date(&mut line, ship_date);
                } else {
                    line.push(b'|');
                }
                writeln!(
                    line,
                    "{}|{page}|{}|",
                    page / pages_per_catalog,
                    pricing.get_ship_cost()
                )?;
                lineitems.write_all(&line)?;

                let returned = uniform_int(0, 9999, streams.get(S_CLIN_IS_RETURNED))
                    < CATALOG_RETURN_PER_10000;
                if returned && shipped {
                    let lag = uniform_int(0, 60, return_streams.get(S_CRET_DATE));
                    let time = generate_time_join_key(
                        HoursWeights::Uniform,
                        return_streams.get(S_CRET_TIME),
                    );
                    let call_center = generate_scd_join_key(
                        Table::CallCenter,
                        return_streams.get(S_CRET_CALL_CENTER_ID),
                        1,
                        scaling,
                    );
                    let reason = generate_uniform_join_key(
                        Table::Reason,
                        return_streams.get(S_CRET_REASON_ID),
                        scaling,
                    );
                    let quantity = uniform_int(
                        1,
                        pricing.get_quantity(),
                        return_streams.get(S_CRET_PRICING),
                    );
                    let ship_mode = generate_uniform_join_key(
                        Table::ShipMode,
                        return_streams.get(S_CRET_SHIPMODE_ID),
                        scaling,
                    );
                    let warehouse = generate_uniform_join_key(
                        Table::Warehouse,
                        return_streams.get(S_CRET_WAREHOUSE_ID),
                        scaling,
                    );

                    line.clear();
                    push_id(&mut line, call_center);
                    write!(line, "{order}|{number}|")?;
                    push_id(&mut line, item);
                    push_id(&mut line, ship_customer);
                    push_id(&mut line, ship_customer);
                    push_date(&mut line, ship_date + lag);
                    push_time(&mut line, time);
                    write!(
                        line,
                        "{quantity}|{}|{}|0|{}|0|0|0|",
                        pricing.get_net_paid(),
                        pricing.get_ext_tax(),
                        pricing.get_ext_ship_cost()
                    )?;
                    push_id(&mut line, reason);
                    push_id(&mut line, ship_mode);
                    push_id(&mut line, warehouse);
                    push_id(&mut line, page);
                    line.push(b'\n');
                    returns.write_all(&line)?;
                }
            }

            line.clear();
            write!(line, "{order}|")?;
            push_id(&mut line, bill_customer);
            push_id(&mut line, ship_customer);
            push_date(&mut line, date);
            write!(line, "{time}|")?;
            push_id(&mut line, ship_mode);
            // the call center is never set
            push_id(&mut line, 0);
            writeln!(line, "{comment}|")?;
            orders.write_all(&line)?;

            streams.row_stop();
            return_streams.row_stop();
        }
        Ok(())
    }

    /// `s_web_order_<n>.dat`, `s_web_order_lineitem_<n>.dat` and
    /// `s_web_returns_<n>.dat`
    pub fn write_web<O: Write, L: Write, R: Write>(
        &self,
        orders: &mut O,
        lineitems: &mut L,
        returns: &mut R,
    ) -> io::Result<()> {
        let scaling = &self.scaling;
        let text = RandomTextGenerator::new();
        let mut streams = Streams::new(S_WEB_ORDER_SEEDS);
        let mut return_streams = Streams::new(S_WEB_RETURNS_SEEDS);
        let permutation = make_permutation(self.item_count as usize, streams.get(S_WLIN_PERMUTE));
        let mut line = Vec::new();

        for order in 1..=self.web_order_count() {
            let date = update_date(&self.web_orders, &self.dates, order);
            let time = uniform_int(0, 24 * 3600 - 1, streams.get(S_WORD_ORDER_TIME));
            let (bill_customer, ship_customer) = customers(
                scaling,
                &mut streams,
                S_WORD_BILL_CUSTOMER_ID,
                S_WORD_SHIP_CUSTOMER_ID,
            );
            let ship_mode = generate_uniform_join_key(
                Table::ShipMode,
                streams.get(S_WORD_SHIP_MODE_ID),
                scaling,
            );
            let web_site =
                generate_scd_join_key(Table::WebSite, streams.get(S_WORD_WEB_SITE_ID), 1, scaling);
            let comment = order_comment(&text, streams.get(S_WORD_COMMENT));
            let mut item_index = i64::from(uniform_int(
                1,
                self.item_count as i32,
                streams.get(S_WLIN_ITEM_ID),
            ));

            for number in 1..=WEB_LINES {
                // dsdgen skips ahead instead of wrapping around, past the end
                item_index += 1;
                if item_index > self.item_count {
                    item_index += 1;
                }
                let item = first_surrogate_key(permutation_entry(&permutation, item_index));
                let promotion = generate_uniform_join_key(
                    Table::Promotion,
                    streams.get(S_WLIN_PROMOTION_ID),
                    scaling,
                );
                let warehouse = generate_uniform_join_key(
                    Table::Warehouse,
                    streams.get(S_WLIN_WAREHOUSE_ID),
                    scaling,
                );
                let ship_date = date + uniform_int(0, 60, streams.get(S_WLIN_SHIP_DATE));
                let web_page = generate_scd_join_key(
                    Table::WebPage,
                    streams.get(S_WLIN_WEB_PAGE_ID),
                    1,
                    scaling,
                );
                let pricing = sales_pricing(streams.get(S_WLIN_PRICING));

                line.clear();
                write!(line, "{order}|{number}|")?;
                push_id(&mut line, item);
                push_id(&mut line, promotion);
                write!(
                    line,
                    "{}|{}|{}|",
                    pricing.get_quantity(),
                    pricing.get_sales_price(),
                    pricing.get_coupon_amount()
                )?;
                push_id(&mut line, warehouse);
                if ship_date <= Date::JULIAN_TODAYS_DATE {
                    push_date(&mut line, ship_date);
                } else {
                    line.push(b'|');
                }
                write!(line, "{}|", pricing.get_ship_cost())?;
                push_id(&mut line, web_page);
                line.push(b'\n');
                lineitems.write_all(&line)?;

                if uniform_int(0, 99, streams.get(S_WLIN_IS_RETURNED)) < RETURN_PCT {
                    let lag = uniform_int(1, 60, return_streams.get(S_WRET_RETURN_DATE));
                    let time = generate_time_join_key(
                        HoursWeights::Uniform,
                        return_streams.get(S_WRET_RETURN_TIME),
                    );
                    // drawn, then replaced by the second draw below
                    generate_uniform_join_key(
                        Table::Reason,
                        return_streams.get(S_WRET_REASON_ID),
                        scaling,
                    );
                    let quantity = uniform_int(
                        1,
                        pricing.get_quantity(),
                        return_streams.get(S_WRET_PRICING),
                    );
                    let reason = generate_uniform_join_key(
                        Table::Reason,
                        return_streams.get(S_WRET_REASON_ID),
                        scaling,
                    );

                    line.clear();
                    push_id(&mut line, web_site);
                    write!(line, "{order}|{number}|")?;
                    push_id(&mut line, item);
                    push_id(&mut line, ship_customer);
                    push_id(&mut line, ship_customer);
                    push_date(&mut line, date + lag);
                    push_time(&mut line, time);
                    write!(
                        line,
                        "{quantity}|{}|{}|0|{}|0|0|0|",
                        pricing.get_ext_sales_price(),
                        pricing.get_ext_tax(),
                        pricing.get_ext_ship_cost()
                    )?;
                    push_id(&mut line, reason);
                    line.push(b'\n');
                    returns.write_all(&line)?;
                }
            }

            line.clear();
            write!(line, "{order}|")?;
            push_id(&mut line, bill_customer);
            push_id(&mut line, ship_customer);
            push_date(&mut line, date);
            write!(line, "{time}|")?;
            push_id(&mut line, ship_mode);
            push_id(&mut line, web_site);
            writeln!(line, "{comment}|")?;
            orders.write_all(&line)?;

            streams.row_stop();
            return_streams.row_stop();
        }
        Ok(())
    }

    /// `s_inventory_<n>.dat`
    pub fn write_inventory<W: Write>(&self, out: &mut W) -> io::Result<()> {
        // one draw per row, so no padding
        let mut quantity_stream = RandomNumberStream::new_with_column(S_INVN_QUANTITY, 1);
        let per_date = self.item_count * self.warehouse_count;
        let mut line = Vec::new();
        for row in 0..self.inventory_count() {
            let item = row % self.item_count + 1;
            let warehouse = row / self.item_count % self.warehouse_count + 1;
            let date = self.inventory_dates[((row / per_date) as usize).min(UPDATE_DATES - 1)];
            let quantity = uniform_int(0, 250, &mut quantity_stream);

            line.clear();
            push_id(&mut line, warehouse);
            push_id(&mut line, first_surrogate_key(item));
            push_date(&mut line, date);
            writeln!(line, "{quantity}|")?;
            out.write_all(&line)?;
        }
        Ok(())
    }
}

/// The sales and inventory dates of update set `number` (dsdgen's
/// `setUpdateDates()`): each set draws on from where the previous one stopped.
fn update_dates(number: i32) -> ([i32; UPDATE_DATES], [i32; UPDATE_DATES]) {
    let mut stream = RandomNumberStream::new_with_column(0, 0);
    let mut dates = [0; UPDATE_DATES];
    let mut inventory_dates = [0; UPDATE_DATES];
    for _ in 0..number {
        for (zone, weights) in [
            CalendarWeights::Low,
            CalendarWeights::Medium,
            CalendarWeights::High,
        ]
        .into_iter()
        .enumerate()
        {
            // two adjacent days in the zone
            let first = skewed_julian_date(weights, &mut stream);
            let second = if day_weight(first, 1, weights) != 0 {
                first + 1
            } else {
                first - 1
            };
            dates[2 * zone] = first;
            dates[2 * zone + 1] = second;

            // the Thursday of the first day's week, moved into the zone
            let first_date = Date::from_julian_days(first);
            let mut thursday = first + (4 - first_date.compute_day_of_week());
            if day_weight(thursday, 0, weights) == 0 {
                thursday -= 7;
                if day_weight(thursday, 0, weights) == 0 {
                    thursday += 14;
                }
            }
            // and the Thursday next to it, also in the zone; the low zone
            // looks at the day after it (sic)
            let mut next_thursday = thursday + 7;
            let offset = if zone == 0 { 1 } else { 0 };
            if day_weight(next_thursday, offset, weights) == 0 {
                next_thursday -= 14;
            }
            inventory_dates[2 * zone] = thursday;
            inventory_dates[2 * zone + 1] = next_thursday;
        }
    }
    (dates, inventory_dates)
}

/// A date with a day of the year drawn by `weights` and a year drawn from
/// 1998 to 2002 (dsdgen's `getSkewedJulianDate()`)
fn skewed_julian_date(weights: CalendarWeights, stream: &mut RandomNumberStream) -> i32 {
    let (month, day) = CalendarDistribution::pick_random_month_and_day(weights, stream);
    let year = uniform_int(Date::DATE_MINIMUM.year(), Date::DATE_MAXIMUM.year(), stream);
    Date::new(year, month, day).to_julian_days()
}

/// The calendar weight of the day `offset` days into the year after `julian`'s
/// (dsdgen's `dist_weight(..., "calendar", day_number(date) + offset, ...)`)
fn day_weight(julian: i32, offset: i32, weights: CalendarWeights) -> i32 {
    let date = Date::from_julian_days(julian);
    CalendarDistribution::get_weight_for_day_number(
        CalendarDistribution::get_index_for_date(&date) + offset,
        weights,
    )
}

/// The date of order `order` (from 1): the update dates take the orders in
/// turn, as many as each date's sales (dsdgen's `getUpdateDate()`)
fn update_date(orders: &[i64; UPDATE_DATES], dates: &[i32; UPDATE_DATES], order: i64) -> i32 {
    let index = orders
        .iter()
        .position(|&orders| order <= orders)
        .unwrap_or(UPDATE_DATES - 1);
    dates[index]
}

/// The item counter after `steps` line items, wrapping from the last item to
/// the first
fn advance_item_index(index: i64, steps: i64, item_count: i64) -> i64 {
    if steps == 0 {
        return index;
    }
    let index = index + steps;
    if index <= item_count {
        index
    } else {
        (index - 1) % item_count + 1
    }
}

/// The item at `index` of the permutation (from 1); outside it, the 0 dsdgen
/// reads before the permutation and past its end
fn permutation_entry(permutation: &[i32], index: i64) -> i64 {
    let entry = usize::try_from(index - 1)
        .ok()
        .and_then(|index| permutation.get(index))
        .copied()
        .unwrap_or(0);
    i64::from(entry) + 1
}

/// The first surrogate key of an item ID (dsdgen's `getFirstSK()`)
fn first_surrogate_key(id: i64) -> i64 {
    match id % 3 {
        1 => id / 3 * 6 + 1,
        2 => id / 3 * 6 + 2,
        _ => (id / 3 - 1) * 6 + 4,
    }
}

/// The billing customer, and the shipping customer (the billing customer
/// unless the order is a gift)
fn customers(scaling: &Scaling, streams: &mut Streams, bill: i32, ship: i32) -> (i64, i64) {
    let bill_customer = generate_uniform_join_key(Table::Customer, streams.get(bill), scaling);
    let ship_customer = if uniform_int(0, 99, streams.get(ship)) > GIFT_PCT {
        generate_uniform_join_key(Table::Customer, streams.get(ship), scaling)
    } else {
        bill_customer
    };
    (bill_customer, ship_customer)
}

fn order_comment(text: &RandomTextGenerator, stream: &mut RandomNumberStream) -> String {
    text.random_text(COMMENT_LENGTH * 6 / 10, COMMENT_LENGTH, stream)
}

/// A line item's prices, with the web sales limits all line items use
fn sales_pricing(stream: &mut RandomNumberStream) -> Pricing {
    generate_pricing_for_sales_table(&get_web_sales_pricing_limits(), stream)
}

fn uniform_int(min: i32, max: i32, stream: &mut RandomNumberStream) -> i32 {
    RandomValueGenerator::generate_uniform_random_int(min, max, stream)
}

fn push_id(line: &mut Vec<u8>, key: i64) {
    if key != -1 {
        line.extend_from_slice(make_business_key(key as u64).as_bytes());
    }
    line.push(b'|');
}

fn push_date(line: &mut Vec<u8>, julian: i32) {
    if julian > 0 {
        write!(line, "{}", Date::from_julian_days(julian)).expect("writing to a Vec cannot fail");
    }
    line.push(b'|');
}

fn push_time(line: &mut Vec<u8>, seconds: i64) {
    write!(
        line,
        "{:02}:{:02}:{:02}|",
        seconds / 3600,
        seconds / 60 % 60,
        seconds % 60
    )
    .expect("writing to a Vec cannot fail");
}

fn write_date_pairs<W: Write>(out: &mut W, dates: &[i32; UPDATE_DATES]) -> io::Result<()> {
    for pair in dates.chunks(2) {
        writeln!(
            out,
            "{}|{}|",
            Date::from_julian_days(pair[0]),
            Date::from_julian_days(pair[1])
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_item_index_wraps_to_the_first_item() {
        assert_eq!(advance_item_index(-1, 1, 10), 0);
        assert_eq!(advance_item_index(-1, 11, 10), 10);
        assert_eq!(advance_item_index(-1, 12, 10), 1);
        assert_eq!(advance_item_index(9, 1, 10), 10);
        assert_eq!(advance_item_index(10, 1, 10), 1);
        assert_eq!(advance_item_index(10, 21, 10), 1);
    }

    #[test]
    fn test_first_surrogate_key() {
        assert_eq!(first_surrogate_key(1), 1);
        assert_eq!(first_surrogate_key(2), 2);
        assert_eq!(first_surrogate_key(3), 4);
        assert_eq!(first_surrogate_key(4), 7);
    }

    #[test]
    fn test_permutation_entry_outside_reads_zero() {
        let permutation = [2, 0, 1];
        assert_eq!(permutation_entry(&permutation, 0), 1);
        assert_eq!(permutation_entry(&permutation, 1), 3);
        assert_eq!(permutation_entry(&permutation, 5), 1);
    }
}
