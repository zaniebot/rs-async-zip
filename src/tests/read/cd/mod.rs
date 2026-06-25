// Copyright (c) 2025 Astral
// MIT License (https://github.com/astral-sh/rs-async-zip/blob/main/LICENSE)

#[cfg(feature = "deflate")]
#[tokio::test]
async fn test_nonempty_cd_comment() {
    use futures_lite::io::Cursor;

    use crate::base::read::cd::{CentralDirectoryReader, Entry};
    use crate::base::read::stream::ZipFileReader;
    use crate::tests::init_logger;

    init_logger();

    let data = include_bytes!("nonempty_cd_comment.zip").to_vec();

    let mut cursor = Cursor::new(data);

    let mut zip = ZipFileReader::new(&mut cursor);

    // Move forward through the ZIP's local file entries to reach the CD.
    // We do this instead of using the EOCDR locator to mimic a streaming read.
    let mut offset = 0;
    while let Some(entry) = zip.next_with_entry().await.unwrap() {
        (.., zip) = entry.skip().await.unwrap();
        offset = zip.offset();
    }

    let mut cdr = CentralDirectoryReader::new(&mut cursor, offset);

    let Entry::CentralDirectoryEntry(_) = cdr.next().await.unwrap() else {
        panic!("expected a central directory entry");
    };

    // Our position matches the end of the CD entry, including its
    // non-empty comment field.
    assert_eq!(cursor.position(), 0x2c + 52);
}

#[tokio::test]
async fn cancelled_incremental_cd_record_poisoned_reader_cannot_be_reused() {
    use std::future::Future;
    use std::io;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    use futures_lite::future::poll_fn;
    use futures_lite::io::{AsyncRead, Cursor};

    use crate::base::read::cd::CentralDirectoryReader;
    use crate::base::read::stream::ZipFileReader;

    struct ChunkedPendingReader {
        bytes: Vec<u8>,
        pos: usize,
        pending: bool,
    }

    impl AsyncRead for ChunkedPendingReader {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
            if self.pending {
                self.pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            self.pending = true;
            let len = 5.min(buf.len()).min(self.bytes.len() - self.pos);
            let end = self.pos + len;
            buf[..len].copy_from_slice(&self.bytes[self.pos..end]);
            self.pos = end;
            Poll::Ready(Ok(len))
        }
    }

    let data = include_bytes!("nonempty_cd_comment.zip").to_vec();
    let mut cursor = Cursor::new(&data);
    let mut zip = ZipFileReader::new(&mut cursor);
    let mut offset = 0;
    while let Some(entry) = zip.next_with_entry().await.unwrap() {
        (.., zip) = entry.skip().await.unwrap();
        offset = zip.offset();
    }

    let source = ChunkedPendingReader { bytes: data[offset as usize + 4..].to_vec(), pos: 0, pending: false };
    let mut reader = CentralDirectoryReader::new(source, offset);

    let mut first = Box::pin(reader.next());
    let first_poll = poll_fn(|cx| Poll::Ready(first.as_mut().poll(cx))).await;
    assert!(first_poll.is_pending());
    drop(first);

    assert!(matches!(reader.next().await, Err(crate::error::ZipError::CentralDirectoryReaderPoisoned)));
}
