mod application;
mod domain;
mod epoch_fence;
mod infrastructure;
mod interface;

use clap::Parser;

fn main() {
    use clap::error::ErrorKind;

    let arguments = std::env::args_os().collect::<Vec<_>>();
    let machine_requested = arguments.iter().any(|argument| argument == "--json");
    let cli = match interface::Cli::try_parse_from(arguments) {
        Ok(cli) => cli,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::DisplayHelp
                    | ErrorKind::DisplayVersion
                    | ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            ) =>
        {
            // Help and version are not errors; surface them to stdout and
            // exit 0 even when `--json` is also passed. Consumers that ask
            // for help should not receive an INVALID_ARGUMENT envelope.
            let _ = error.print();
            std::process::exit(0);
        }
        Err(error) if machine_requested => {
            std::process::exit(interface::emit_parse_error(&error.to_string()));
        }
        Err(error) => error.exit(),
    };
    let machine_mode = cli.machine_mode();
    let operation = cli.machine_operation();
    if let Err(error) = interface::run(cli) {
        if machine_mode {
            std::process::exit(interface::emit_machine_error(operation, &error));
        }
        eprintln!("error: {error}");
        std::process::exit(1);
    }
}
