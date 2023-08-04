use dev_util::log::log_init;
use lazy_static::lazy_static;
use parking_lot::RwLock;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio::{
    sync::mpsc::{self, Receiver},
    time,
};

lazy_static! {
    static ref RULE_TX_HANDLE: RwLock<Vec<(String, Sender<String>, JoinHandle<()>)>> =
        RwLock::new(Vec::new());
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    log_init();
    log::info!("dtm start");
    let (tx_main, mut rx_main) = mpsc::channel(1024);
    tokio::spawn(async move {
        for i in 0..100_000 {
            let msg = format!("msg {}", i);
            let _ = tx_main.send(msg).await;
            time::sleep(time::Duration::from_secs(1)).await;
        }
    });

    let mut rule_tx_handle = RULE_TX_HANDLE.write();
    for i in 0..10 {
        let rule_id = format!("rule_{}", i);
        let (tx, rx) = mpsc::channel(1024);
        let rule_id_clone = rule_id.clone();
        let join_handle = tokio::spawn(async move {
            do_something(rule_id_clone, rx).await;
        });
        rule_tx_handle.push((rule_id, tx, join_handle));
    }
    drop(rule_tx_handle);

    tokio::spawn(async move {
        time::sleep(time::Duration::from_secs(5)).await;
        log::info!("remove thread");
        let mut rule_tx_handle = RULE_TX_HANDLE.write();
        loop {
            if rule_tx_handle.len() > 0 {
                let (_, _, join_handle) = &rule_tx_handle[0];
                join_handle.abort();
                rule_tx_handle.remove(0);
            } else {
                break;
            }
        }
        drop(rule_tx_handle);
    });
    while let Some(msg) = rx_main.recv().await {
        for index in 0..RULE_TX_HANDLE.read().len() {
            let (_, tx, _) = &RULE_TX_HANDLE.read()[index];
            let msg_clone = msg.clone();
            let send_result = tx.send(msg_clone).await;
            log::info!("send_result: {:?}", send_result);
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
