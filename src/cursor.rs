//! Cursors lifted from an older version of parquet.
use std::io::{self, Cursor, Error, ErrorKind, Read, Seek, SeekFrom, Write};
use std::sync::{Arc, Mutex};
use std::{cmp, fmt};

#[allow(clippy::rc_buffer)]
/// A SliceableCursor lifted from the legacy parquet implementation
pub struct SliceableCursor {
    inner: Arc<Vec<u8>>,
    start: u64,
    length: usize,
    pos: u64,
}

impl fmt::Debug for SliceableCursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SliceableCursor")
            .field("start", &self.start)
            .field("length", &self.length)
            .field("pos", &self.pos)
            .field("inner.len", &self.inner.len())
            .finish()
    }
}

impl SliceableCursor {
    /// Create a new SliceableCursor
    pub fn new(content: impl Into<Arc<Vec<u8>>>) -> Self {
        let inner = content.into();
        let size = inner.len();
        SliceableCursor {
            inner,
            start: 0,
            pos: 0,
            length: size,
        }
    }

    /// Create a slice cursor using the same data as a current one.
    pub fn slice(&self, start: u64, length: usize) -> io::Result<Self> {
        let new_start = self.start + start;
        if new_start >= self.inner.len() as u64 || new_start as usize + length > self.inner.len() {
            return Err(Error::new(ErrorKind::InvalidInput, "out of bound"));
        }
        Ok(SliceableCursor {
            inner: Arc::clone(&self.inner),
            start: new_start,
            pos: new_start,
            length,
        })
    }

    fn remaining_slice(&self) -> &[u8] {
        let end = self.start as usize + self.length;
        let offset = cmp::min(self.pos, end as u64) as usize;
        &self.inner[offset..end]
    }

    /// Get the length of the current cursor slice
    pub fn len(&self) -> u64 {
        self.length as u64
    }

    /// return true if the cursor is empty (self.len() == 0)
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Implementation inspired by std::io::Cursor
impl Read for SliceableCursor {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = Read::read(&mut self.remaining_slice(), buf)?;
        self.pos += n as u64;
        Ok(n)
    }
}

impl Seek for SliceableCursor {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(pos) => pos as i64,
            SeekFrom::End(pos) => self.inner.len() as i64 + pos,
            SeekFrom::Current(pos) => self.pos as i64 + pos,
        };

        if new_pos < 0 {
            Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Request out of bounds: cur position {} + seek {:?} < 0: {}",
                    self.pos, pos, new_pos
                ),
            ))
        } else if new_pos >= self.inner.len() as i64 {
            Err(Error::new(
                ErrorKind::InvalidInput,
                format!(
                    "Request out of bounds: cur position {} + seek {:?} >= length {}: {}",
                    self.pos,
                    pos,
                    self.inner.len(),
                    new_pos
                ),
            ))
        } else {
            self.pos = new_pos as u64;
            Ok(self.start)
        }
    }
}

#[derive(Debug, Default, Clone)]
pub(crate) struct InMemoryWriteableCursor {
    buffer: Arc<Mutex<Cursor<Vec<u8>>>>,
}

impl InMemoryWriteableCursor {
    /// Returns a clone of the underlying buffer
    pub fn data(&self) -> Vec<u8> {
        let inner = self.buffer.lock().unwrap();
        inner.get_ref().to_vec()
    }
}

impl Write for InMemoryWriteableCursor {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let mut inner = self.buffer.lock().unwrap();
        inner.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut inner = self.buffer.lock().unwrap();
        inner.flush()
    }
}

impl Seek for InMemoryWriteableCursor {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        let mut inner = self.buffer.lock().unwrap();
        inner.seek(pos)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Create a SliceableCursor of all u8 values in ascending order
    fn get_u8_range() -> SliceableCursor {
        let data: Vec<u8> = (0u8..=255).collect();
        SliceableCursor::new(data)
    }

    /// Reads all the bytes in the slice and checks that it matches the u8 range from start to end_included
    fn check_read_all(mut cursor: SliceableCursor, start: u8, end_included: u8) {
        let mut target = vec![];
        let cursor_res = cursor.read_to_end(&mut target);
        println!("{:?}", cursor_res);
        assert!(cursor_res.is_ok(), "reading error");
        assert_eq!((end_included - start) as usize + 1, cursor_res.unwrap());
        assert_eq!((start..=end_included).collect::<Vec<_>>(), target);
    }

    #[test]
    fn read_all_whole() {
        let cursor = get_u8_range();
        check_read_all(cursor, 0, 255);
    }

    #[test]
    fn read_all_slice() {
        let cursor = get_u8_range().slice(10, 10).expect("error while slicing");
        check_read_all(cursor, 10, 19);
    }

    #[test]
    fn seek_cursor_start() {
        let mut cursor = get_u8_range();

        cursor.seek(SeekFrom::Start(5)).unwrap();
        check_read_all(cursor, 5, 255);
    }

    #[test]
    fn seek_cursor_current() {
        let mut cursor = get_u8_range();
        cursor.seek(SeekFrom::Start(10)).unwrap();
        cursor.seek(SeekFrom::Current(10)).unwrap();
        check_read_all(cursor, 20, 255);
    }

    #[test]
    fn seek_cursor_end() {
        let mut cursor = get_u8_range();

        cursor.seek(SeekFrom::End(-10)).unwrap();
        check_read_all(cursor, 246, 255);
    }

    #[test]
    fn seek_cursor_error_too_long() {
        let mut cursor = get_u8_range();
        let res = cursor.seek(SeekFrom::Start(1000));
        let actual_error = res.expect_err("expected error").to_string();
        let expected_error =
            "Request out of bounds: cur position 0 + seek Start(1000) >= length 256: 1000";
        assert_eq!(actual_error, expected_error);
    }

    #[test]
    fn seek_cursor_error_too_short() {
        let mut cursor = get_u8_range();
        let res = cursor.seek(SeekFrom::End(-1000));
        let actual_error = res.expect_err("expected error").to_string();
        let expected_error = "Request out of bounds: cur position 0 + seek End(-1000) < 0: -744";
        assert_eq!(actual_error, expected_error);
    }
}
