//! `tpcds updates`: the TPC-DS refresh data sets (`dsdgen -update <n>`).

use crate::args::{default_num_threads, parse_scale_factor};
use crate::logging::configure_logging;
use log::info;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;
use std::thread;
use std::time::Instant;
use tpcdsgen::maintenance::UpdateSet;

/// Options of `tpcds updates`
#[derive(clap::Args)]
#[command(next_help_heading = "Update Options")]
pub(super) struct UpdatesArgs {
    /// Scale factor of the data the update sets apply to
    #[arg(short, long, default_value_t = 1., value_parser = parse_scale_factor)]
    scale_factor: f64,

    /// Number of update sets to generate, 1 to N (`dsdgen -update <n>` for each)
    #[arg(
        long,
        default_value_t = 1,
        value_name = "N",
        value_parser = clap::value_parser!(i32).range(1..)
    )]
    sets: i32,

    /// Output directory for generated files
    #[arg(short, long, default_value = ".")]
    output_dir: PathBuf,

    /// Overwrite output files that already exist.
    #[arg(long, default_value_t = false)]
    overwrite: bool,

    /// Number of threads to use [default: number of CPUs]
    #[arg(
        short,
        long,
        default_value_t = default_num_threads(),
        hide_default_value = true,
        value_parser = clap::builder::RangedU64ValueParser::<usize>::new().range(1..)
    )]
    num_threads: usize,

    /// Log at info level (overrides RUST_LOG)
    #[arg(short, long, default_value_t = false, conflicts_with = "quiet")]
    verbose: bool,

    /// Only log errors
    #[arg(short, long, default_value_t = false, conflicts_with = "verbose")]
    quiet: bool,
}

/// The files of an update set, each written by one task
#[derive(Clone, Copy)]
enum Task {
    Store,
    Catalog,
    Web,
    Inventory,
    Deletes,
}

const TASKS: [Task; 5] = [
    // the largest first
    Task::Inventory,
    Task::Store,
    Task::Catalog,
    Task::Web,
    Task::Deletes,
];

impl UpdatesArgs {
    pub(super) fn run(self) -> Result<(), Box<dyn std::error::Error>> {
        configure_logging(self.verbose, self.quiet, None);
        let start = Instant::now();
        info!(
            "Generating TPC-DS update sets 1 to {} (SF={}) to {}",
            self.sets,
            self.scale_factor,
            self.output_dir.display()
        );
        fs::create_dir_all(&self.output_dir)?;

        let sets: Vec<UpdateSet> = (1..=self.sets)
            .map(|number| UpdateSet::new(self.scale_factor, number))
            .collect();
        let tasks: Vec<(i32, &UpdateSet, Task)> = TASKS
            .iter()
            .flat_map(|&task| {
                (1..)
                    .zip(&sets)
                    .map(move |(number, set)| (number, set, task))
            })
            .collect();

        let next = AtomicUsize::new(0);
        let first_error = Mutex::new(None);
        thread::scope(|scope| {
            for _ in 0..self.num_threads.min(tasks.len()) {
                scope.spawn(|| {
                    while let Some(&(number, set, task)) =
                        tasks.get(next.fetch_add(1, Ordering::Relaxed))
                    {
                        if let Err(error) = self.write(number, set, task) {
                            first_error.lock().unwrap().get_or_insert(error);
                        }
                    }
                });
            }
        });
        if let Some(error) = first_error.into_inner().unwrap() {
            return Err(error.into());
        }

        info!("Generation complete in {:.2?}!", start.elapsed());
        Ok(())
    }

    fn write(&self, number: i32, set: &UpdateSet, task: Task) -> io::Result<()> {
        let file = |table: &str| self.create(&format!("{table}_{number}.dat"));
        match task {
            Task::Store => {
                let (mut orders, mut lineitems, mut returns) = (
                    file("s_purchase")?,
                    file("s_purchase_lineitem")?,
                    file("s_store_returns")?,
                );
                set.write_store(&mut orders, &mut lineitems, &mut returns)?;
                finish([orders, lineitems, returns])
            }
            Task::Catalog => {
                let (mut orders, mut lineitems, mut returns) = (
                    file("s_catalog_order")?,
                    file("s_catalog_order_lineitem")?,
                    file("s_catalog_returns")?,
                );
                set.write_catalog(&mut orders, &mut lineitems, &mut returns)?;
                finish([orders, lineitems, returns])
            }
            Task::Web => {
                let (mut orders, mut lineitems, mut returns) = (
                    file("s_web_order")?,
                    file("s_web_order_lineitem")?,
                    file("s_web_returns")?,
                );
                set.write_web(&mut orders, &mut lineitems, &mut returns)?;
                finish([orders, lineitems, returns])
            }
            Task::Inventory => {
                let mut inventory = file("s_inventory")?;
                set.write_inventory(&mut inventory)?;
                finish([inventory])
            }
            Task::Deletes => {
                let (mut delete, mut inventory_delete) =
                    (file("delete")?, file("inventory_delete")?);
                set.write_delete(&mut delete)?;
                set.write_inventory_delete(&mut inventory_delete)?;
                finish([delete, inventory_delete])
            }
        }
    }

    fn create(&self, name: &str) -> io::Result<BufWriter<File>> {
        let path = self.output_dir.join(name);
        let file = open(&path, self.overwrite).map_err(|error| {
            io::Error::new(error.kind(), format!("{}: {error}", path.display()))
        })?;
        Ok(BufWriter::with_capacity(1 << 20, file))
    }
}

fn open(path: &Path, overwrite: bool) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.write(true);
    if overwrite {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    options.open(path)
}

fn finish<const N: usize>(files: [BufWriter<File>; N]) -> io::Result<()> {
    for mut file in files {
        file.flush()?;
    }
    Ok(())
}
