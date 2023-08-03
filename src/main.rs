use dev_util::log::log_init;
use tokio::{
    sync::mpsc::{self, Receiver},
    time,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    log_init();
    log::info!("dtm start");
    let (tx_main, mut rx_main) = mpsc::channel(1024);
    tokio::spawn(async move {
        for i in 0..100_000 {
            let msg = format!("msg {}", i);
            let _ = tx_main.send(msg).await;
        }
    });

    let mut threads = Vec::new();

    for i in 0..10 {
        let rule_id = format!("rule_{}", i);
        let (tx, rx) = mpsc::channel(1024);
        let rule_id_clone = rule_id.clone();
        let join_handle = tokio::spawn(async move {
            do_something(rule_id_clone, rx).await;
        });
        threads.push((rule_id, tx, join_handle));
    }


    while let Some(msg) = rx_main.recv().await {
        for (_, tx, _) in &threads {
            let msg_clone = msg.clone();
            let _ = tx.send(msg_clone).await;
        }
        // time::sleep(time::Duration::from_secs(3)).await;
        // log::info!("join_handle abort");
        // join_handle.abort();
        // log::info!(
        //     "is_cancelled: {}",
        //     join_handle.await.unwrap_err().is_cancelled()
        // );
        // time::sleep(time::Duration::from_secs(10)).await;
    }

    time::sleep(time::Duration::from_secs(10)).await;
    log::info!("dtm end");
    Ok(())
}

async fn do_something(rule_id: String, mut rx: Receiver<String>) {
    log::info!("do_something start: {}", rule_id);
    while let Some(msg) = rx.recv().await {
        log::info!("do_something running: {}-{}", rule_id, msg);
        time::sleep(time::Duration::from_secs(1)).await;
    }
}
