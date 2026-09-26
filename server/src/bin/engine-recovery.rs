//! Offline recovery control. Never accepts an unsafe gate-reset operation.
use engine_core::recovery::RecoveryJournal;
use std::{collections::BTreeMap, path::PathBuf};

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    let command=args.next().ok_or("Usage: engine-recovery <init|status|export|quarantine|recover|reattach> --path PRIVATE_LEDGER [--namespace NS | --new-namespace NS] [--out PRIVATE_EXPORT]")?;
    let mut flags = BTreeMap::new();
    while let Some(flag) = args.next() {
        if !matches!(
            flag.as_str(),
            "--path" | "--namespace" | "--new-namespace" | "--out"
        ) || flags.contains_key(&flag)
        {
            return Err("Unknown or duplicate option".into());
        }
        flags.insert(flag, args.next().ok_or("Option requires a value")?);
    }
    let path = PathBuf::from(flags.remove("--path").ok_or("--path is required")?);
    let namespace = flags.remove("--namespace");
    let new_namespace = flags.remove("--new-namespace");
    let output = flags.remove("--out");
    let redis_url =
        || std::env::var("REDIS_URL").map_err(|_| "REDIS_URL is required for this operation");
    let status = match command.as_str() {
        "init" if output.is_none() && new_namespace.is_none() => {
            RecoveryJournal::initialize(&path, &redis_url()?, namespace).await?;
            RecoveryJournal::status(&path)?
        }
        "status" if output.is_none() && namespace.is_none() && new_namespace.is_none() => {
            RecoveryJournal::status(&path)?
        }
        "export" if namespace.is_none() && new_namespace.is_none() => {
            RecoveryJournal::export(
                &path,
                output.ok_or("--out is required; exports contain sensitive recovery data")?,
            )?;
            println!("Private recovery export written");
            return Ok(());
        }
        "quarantine" if output.is_none() && namespace.is_none() && new_namespace.is_none() => {
            RecoveryJournal::quarantine(&path)?;
            RecoveryJournal::status(&path)?
        }
        "recover" if output.is_none() && namespace.is_none() => {
            RecoveryJournal::recover(
                &path,
                &redis_url()?,
                Some(new_namespace.ok_or("--new-namespace is required")?),
            )
            .await?
        }
        "reattach" if output.is_none() && new_namespace.is_none() => {
            RecoveryJournal::reattach(&path, &redis_url()?, namespace).await?
        }
        _ => return Err("Unknown command or options incompatible with command".into()),
    };
    // Status contains counts and deployment identity only, never payloads/credentials.
    println!("{}", serde_json::to_string_pretty(&status)?);
    Ok(())
}
