// Copyright (c) 2026 Alejandro Gonzales-Irribarren <alejandrxgzi@gmail.com>
// Distributed under the terms of the Apache License, Version 2.0.

mod cli;

use clap::Parser;
use std::io::{self, BufReader, BufWriter, Write};

fn main() {
    let cli = cli::Cli::parse();

    let stdin = io::stdin();
    let mut stdin = BufReader::new(stdin.lock());
    let mut stdout = BufWriter::with_capacity(1024 * 1024, io::stdout().lock());
    let mut stderr = io::stderr().lock();

    let exit_code = match cli::run(cli, &mut stdin, &mut stdout, &mut stderr) {
        Ok(()) => 0,
        Err(err) => {
            let _ = writeln!(stderr, "ERROR: {err}");
            1
        }
    };

    let _ = stdout.flush();
    std::process::exit(exit_code);
}
