use deltalake::storage::s3::dynamodb_lock::{DynamoDbLockClient, DynamoDbOptions, DynamoError};
use rusoto_core::Region;
use rusoto_dynamodb::DynamoDbClient;
use std::time::Duration;

/// Errors returned by [`try_recover_non_acquirable_dynamodb_lock`]
#[derive(thiserror::Error, Debug)]
pub enum RecoverDynamodbLockError {
    /// Error from [`deltalake::storage::s3::dynamodb_lock::DynamoError`]
    #[error("Dynamodb error: {source}")]
    DynamoError {
        /// Wrapped [`deltalake::storage::s3::dynamodb_lock::DynamoError`]
        #[from]
        source: DynamoError,
    },
}

/// Recovers the non-acquirable dynamodb lock if one is found.
///
/// By default dynamodb lock once acquired could be either released by its owner or expired by
/// other writers. However there's a type of locks that could not be expired or acquired by other
/// writers, which is the non-acquirable lock. In order to create one, the owner has to set the
/// `is_non_acquirable` flag once acquire the lock. Other writers will fail to to try to acquire
/// the lock with this flag.
///
/// In order to recover from this lock, KDI ensures that it is released first and waits at least
/// the `min_wait_period` which is usually the write interval (`allowed_latency`). The additional
/// pause before deletion is required to ensure that other writers will face the non-acquirable lock
/// as well, which should happen in between `min_wait_period` interval.
///
/// The possible outcome if the lock is "recovered" by at least one worker was not failed before,
/// could be that the `is_non_acquirable` lock was acquired to perform drop or drop/create scenario.
/// If table is dropped but worker is still active, it'll keep writing despite there's no table;
/// when table is recreated, the worker will keep writer with N+1 version, however the table is on
/// version 0, since the delta log is cleared on drop.
pub async fn try_recover_non_acquirable_dynamodb_lock(
    min_wait_period: Duration,
) -> Result<(), RecoverDynamodbLockError> {
    let dynamodb = DynamoDbClient::new(Region::default());
    let opts = DynamoDbOptions::default();
    let refresh_period = opts.refresh_period;
    let lock_client = DynamoDbLockClient::new(dynamodb, opts);
    recover_dynamodb_lock(&lock_client, min_wait_period, refresh_period).await
}

/// See [`try_recover_non_acquirable_dynamodb_lock`]
pub async fn recover_dynamodb_lock(
    lock_client: &DynamoDbLockClient,
    min_wait_period: Duration,
    refresh_period: Duration,
) -> Result<(), RecoverDynamodbLockError> {
    loop {
        match lock_client.get_lock().await? {
            None => {
                // no lock, no recovery
                log::info!("There is no active lock to recover.");
                return Ok(());
            }
            Some(item) if !item.is_non_acquirable => {
                // lock is present but it's acquirable
                log::info!("There is active lock but it is acquirable, no recover required.");
                return Ok(());
            }
            Some(item) if item.is_non_acquirable && !item.is_released => {
                // lock is present and non-acquirable but not yet released, let's wait
                log::error!("There is active non-acquirable lock found, but it is not released yet, waiting...");
                tokio::time::sleep(refresh_period).await;
                // TODO should we ever expire such lock?
            }
            Some(item) if item.is_non_acquirable && item.is_released => {
                log::error!(
                    "There is active non-acquirable lock found and it is released. \
                    Deleting the lock after {} seconds to ensure that other writers are faced the lock too.",
                    min_wait_period.as_secs()
                );
                tokio::time::sleep(min_wait_period).await;
                if lock_client.delete_lock(&item).await? {
                    log::error!("Successfully deleted the non-acquirable lock");
                    return Ok(());
                } else {
                    log::error!("Could not delete the lock, it could be either deleted manually or by other writers. Trying again.");
                }
            }
            _ => {
                unreachable!()
            }
        }
    }
}
