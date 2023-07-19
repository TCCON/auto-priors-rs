use tokio::process::Command;

mod jobs;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut fib = Command::new("python")
        .arg("fib.py")
        .spawn()
        .unwrap();

    let mut fact = Command::new("python")
        .arg("factorial.py")
        .spawn()
        .unwrap();

    fib.wait().await.unwrap();
    fact.wait().await.unwrap();
    Ok(())
}