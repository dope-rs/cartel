use crate::options::{Checkpoint, Journal, OpenOptions};
use crate::{api, database, open::journal};
use api::work;
use work::call::Call;

impl OpenOptions {
    pub(crate) fn set_journal(&self, call: &Call<'_, '_, '_>) -> Result<(), crate::Error> {
        let (sql, expected) = match self.journal {
            Journal::Rollback => ("PRAGMA journal_mode=DELETE", journal::Mode::Delete),
            Journal::Wal(_) => ("PRAGMA journal_mode=WAL", journal::Mode::Wal),
        };
        let actual = call.journal(sql)?;
        if actual != expected {
            return Err(crate::Error::Journal { expected, actual });
        }
        Ok(())
    }

    pub(crate) fn retain(&self, call: &Call<'_, '_, '_>) -> Result<(), crate::Error> {
        if let Journal::Wal(wal) = self.journal
            && let Some(value) = wal.retained_bytes
        {
            if value > i64::MAX as u64 {
                return Err(crate::Error::JournalBytes { value });
            }
            let bytes = value as i64;
            call.pragma("journal_size_limit", bytes)?;
        }
        Ok(())
    }

    pub(crate) fn set_mapped(
        &self,
        call: &Call<'_, '_, '_>,
        kind: database::Kind,
    ) -> Result<(), crate::Error> {
        call.pragma("mmap_size", self.mapped(kind)?)
    }

    pub(crate) fn sync(&self, call: &Call<'_, '_, '_>) -> Result<(), crate::Error> {
        call.pragma("synchronous", i64::from(self.synchronous.value()))
    }

    pub(crate) fn checkpoint(&self, call: &Call<'_, '_, '_>) -> Result<(), crate::Error> {
        if let Journal::Wal(wal) = self.journal {
            let pages = match wal.checkpoint {
                Checkpoint::Auto => 1_000,
                Checkpoint::Pages(pages) => pages.get(),
                Checkpoint::Manual => 0,
            };
            call.pragma("wal_autocheckpoint", i64::from(pages))?;
        }
        Ok(())
    }

    pub(crate) fn verify(&self, call: &Call<'_, '_, '_>) -> Result<(), crate::Error> {
        let Journal::Wal(_) = self.journal else {
            return Ok(());
        };
        let expected = journal::Mode::Wal;
        let actual = call.journal("PRAGMA journal_mode")?;
        if actual != expected {
            return Err(crate::Error::Journal { expected, actual });
        }
        Ok(())
    }

    pub(crate) fn mapped(&self, kind: database::Kind) -> Result<i64, crate::Error> {
        match kind {
            database::Kind::Memory => Ok(0),
            database::Kind::File => {
                let value = self.mapped_bytes;
                if value > i64::MAX as u64 {
                    return Err(crate::Error::MappedBytes { value });
                }
                Ok(value as i64)
            }
        }
    }
}
