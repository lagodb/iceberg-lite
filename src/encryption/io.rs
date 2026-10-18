// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Encrypted file wrappers for InputFile / OutputFile.

use std::io::Write;
use std::sync::Arc;

use bytes::Bytes;

use super::crypto::AesGcmCipher;
use super::key_metadata::StandardKeyMetadata;
use super::stream::{AesGcmFileRead, AesGcmFileWrite};
use crate::Result;
use crate::error::invalid_data;
use crate::io::{
    FileMetadata, FileRead, FileWrite, InputFile, OpenedFile, OutputFile,
};

pub struct EncryptedInputFile {
    inner: InputFile,
    key_metadata: StandardKeyMetadata,
}

impl EncryptedInputFile {
    pub fn new(inner: InputFile, key_metadata: StandardKeyMetadata) -> Self {
        Self {
            inner,
            key_metadata,
        }
    }

    pub fn location(&self) -> &str {
        self.inner.location()
    }

    pub fn exists(&self) -> Result<bool> {
        self.inner.exists()
    }

    pub fn metadata(&self) -> Result<FileMetadata> {
        let plaintext_size =
            AesGcmFileRead::calculate_plaintext_length(self.encrypted_length()?)?;
        Ok(FileMetadata {
            size: plaintext_size,
        })
    }

    fn encrypted_length(&self) -> Result<u64> {
        self.key_metadata.file_length().ok_or_else(|| {
            invalid_data!("AGS1 key metadata is missing the encrypted file length")
        })
    }

    pub fn read(&self) -> Result<Bytes> {
        self.reader()?.read_all()
    }

    /// Opens one decrypting reader and returns its plaintext length.
    pub fn open_reader(&self) -> Result<OpenedFile> {
        let encrypted_length = self.encrypted_length()?;
        let plaintext_size =
            AesGcmFileRead::calculate_plaintext_length(encrypted_length)?;
        let opened = self.inner.open_reader()?;
        let cipher = build_cipher(&self.key_metadata)?;
        let aad_prefix: Box<[u8]> =
            self.key_metadata.aad_prefix().unwrap_or_default().into();
        let decrypting =
            AesGcmFileRead::new(opened.reader, cipher, aad_prefix, encrypted_length)?;
        Ok(OpenedFile {
            metadata: FileMetadata {
                size: plaintext_size,
            },
            reader: Box::new(decrypting),
        })
    }

    pub fn reader(&self) -> Result<Box<dyn FileRead>> {
        Ok(self.open_reader()?.reader)
    }

    pub fn key_metadata(&self) -> &StandardKeyMetadata {
        &self.key_metadata
    }

    pub fn into_inner(self) -> InputFile {
        self.inner
    }
}

impl std::fmt::Debug for EncryptedInputFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedInputFile")
            .field("path", &self.inner.location())
            .finish_non_exhaustive()
    }
}

pub struct EncryptedOutputFile {
    inner: OutputFile,
    key_metadata: StandardKeyMetadata,
}

impl EncryptedOutputFile {
    pub fn new(inner: OutputFile, key_metadata: StandardKeyMetadata) -> Self {
        Self {
            inner,
            key_metadata,
        }
    }

    pub fn key_metadata(&self) -> &StandardKeyMetadata {
        &self.key_metadata
    }

    pub fn location(&self) -> &str {
        self.inner.location()
    }

    pub fn writer(&self) -> Result<Box<dyn FileWrite>> {
        let raw_writer = self.inner.create_file_writer()?;
        let cipher = build_cipher(&self.key_metadata)?;
        let aad_prefix: Box<[u8]> =
            self.key_metadata.aad_prefix().unwrap_or_default().into();
        Ok(Box::new(AesGcmFileWrite::new(
            raw_writer, cipher, aad_prefix,
        )))
    }

    /// Returns key metadata containing the size returned by the completed writer.
    pub fn key_metadata_with_saved_file_metadata(
        &self,
        metadata: &FileMetadata,
    ) -> StandardKeyMetadata {
        self.key_metadata.clone().with_file_length(metadata.size)
    }

    pub fn write(&self, bs: Bytes) -> Result<FileMetadata> {
        let mut writer = self.writer()?;
        writer.write_all(&bs)?;
        writer.close()
    }

    pub fn delete(&self) -> Result<()> {
        self.inner.delete()
    }

    pub fn into_inner(self) -> OutputFile {
        self.inner
    }
}

impl std::fmt::Debug for EncryptedOutputFile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptedOutputFile")
            .field("path", &self.inner.location())
            .finish_non_exhaustive()
    }
}

fn build_cipher(metadata: &StandardKeyMetadata) -> Result<Arc<AesGcmCipher>> {
    Ok(Arc::new(AesGcmCipher::new(
        metadata.encryption_key().clone(),
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ErrorKind;
    use crate::encryption::stream::{
        CIPHER_BLOCK_SIZE, GCM_STREAM_HEADER_LENGTH, MIN_STREAM_LENGTH,
        PLAIN_BLOCK_SIZE,
    };
    use crate::io::FileIO;

    fn key_metadata() -> StandardKeyMetadata {
        StandardKeyMetadata::try_new(b"0123456789abcdef")
            .unwrap()
            .with_aad_prefix(b"test-aad-prefix!")
    }

    #[test]
    fn test_write_read_roundtrip() {
        let fileio = FileIO::memory();
        let path = "memory:///test/io_roundtrip.bin";
        let plaintext = b"Hello from EncryptedInputFile/EncryptedOutputFile!";

        let output = EncryptedOutputFile::new(
            fileio.new_output(path).unwrap(),
            key_metadata(),
        );
        let file_metadata = output.write(Bytes::from(plaintext.to_vec())).unwrap();

        let input = EncryptedInputFile::new(
            fileio.new_input(path).unwrap(),
            output.key_metadata_with_saved_file_metadata(&file_metadata),
        );
        let content = input.read().unwrap();
        assert_eq!(&content[..], plaintext);
    }

    #[test]
    fn test_metadata_returns_plaintext_size() {
        let fileio = FileIO::memory();
        let path = "memory:///test/io_metadata.bin";
        let plaintext = b"some bytes to measure";

        let output = EncryptedOutputFile::new(
            fileio.new_output(path).unwrap(),
            key_metadata(),
        );
        let file_metadata = output.write(Bytes::from(plaintext.to_vec())).unwrap();

        let raw_size = fileio.new_input(path).unwrap().metadata().unwrap().size;
        assert!(
            raw_size > plaintext.len() as u64,
            "encrypted file should be larger than plaintext (header + nonce + tag)"
        );

        // A missing path proves the size comes from the key metadata rather than a stat call.
        let input = EncryptedInputFile::new(
            fileio.new_input("memory:///does-not-exist").unwrap(),
            output.key_metadata_with_saved_file_metadata(&file_metadata),
        );
        let meta = input.metadata().unwrap();
        assert_eq!(meta.size, plaintext.len() as u64);
    }

    #[test]
    fn test_missing_file_length_is_rejected() {
        let fileio = FileIO::memory();
        let path = "memory:///test/missing_length.bin";
        let output = EncryptedOutputFile::new(
            fileio.new_output(path).unwrap(),
            key_metadata(),
        );
        output.write(Bytes::from_static(b"data")).unwrap();
        let input =
            EncryptedInputFile::new(fileio.new_input(path).unwrap(), key_metadata());

        for err in [
            input.metadata().err().unwrap(),
            input.reader().err().unwrap(),
            input.read().unwrap_err(),
        ] {
            assert_eq!(err.kind(), ErrorKind::DataInvalid);
            assert!(
                err.to_string()
                    .contains("missing the encrypted file length")
            );
        }
    }

    #[test]
    fn test_invalid_file_length_is_rejected() {
        let fileio = FileIO::memory();
        for length in [
            0,
            u64::from(GCM_STREAM_HEADER_LENGTH),
            u64::from(MIN_STREAM_LENGTH - 1),
        ] {
            let input = EncryptedInputFile::new(
                fileio
                    .new_input("memory:///test/invalid_length.bin")
                    .unwrap(),
                key_metadata().with_file_length(length),
            );
            assert_eq!(
                input.metadata().err().unwrap().kind(),
                ErrorKind::DataInvalid
            );
            assert_eq!(input.reader().err().unwrap().kind(), ErrorKind::DataInvalid);
        }
    }

    #[test]
    fn test_oversized_file_length_is_rejected() {
        let fileio = FileIO::memory();
        let path = "memory:///test/oversized_length.bin";
        let plaintext = Bytes::from_static(b"some bytes to measure");
        let output = EncryptedOutputFile::new(
            fileio.new_output(path).unwrap(),
            key_metadata(),
        );
        let file_metadata = output.write(plaintext.clone()).unwrap();

        // A declared length is trusted without a stat, so an inflated one is only caught once a
        // read runs off the end of the real file. Both a minimal overstatement and one spanning a
        // whole extra block must fail rather than silently return short plaintext.
        for excess in [1, u64::from(CIPHER_BLOCK_SIZE)] {
            let input = EncryptedInputFile::new(
                fileio.new_input(path).unwrap(),
                key_metadata().with_file_length(file_metadata.size + excess),
            );

            let inflated_size = input.metadata().unwrap().size;
            assert!(inflated_size > plaintext.len() as u64);

            assert!(input.read().is_err());

            // Not even the bytes that genuinely are on disk can be read back.
            let reader = input.reader().unwrap();
            assert!(reader.read_range(0..plaintext.len() as u64).is_err());
        }
    }

    #[test]
    fn test_truncated_file_is_rejected() {
        let fileio = FileIO::memory();
        let path = "memory:///test/truncated.bin";
        let plaintext = Bytes::from(vec![42; 2 * PLAIN_BLOCK_SIZE as usize + 17]);
        let output = EncryptedOutputFile::new(
            fileio.new_output(path).unwrap(),
            key_metadata(),
        );
        let file_metadata = output.write(plaintext.clone()).unwrap();
        let metadata = key_metadata().with_file_length(file_metadata.size);
        let ciphertext = fileio.new_input(path).unwrap().read().unwrap();
        let truncated_length =
            (GCM_STREAM_HEADER_LENGTH + CIPHER_BLOCK_SIZE) as usize;
        fileio
            .new_output(path)
            .unwrap()
            .write(&ciphertext.slice(..truncated_length))
            .unwrap();

        let input =
            EncryptedInputFile::new(fileio.new_input(path).unwrap(), metadata);
        assert_eq!(input.metadata().unwrap().size, plaintext.len() as u64);
        let reader = input.reader().unwrap();
        assert_eq!(
            reader.read_range(0..u64::from(PLAIN_BLOCK_SIZE)).unwrap(),
            plaintext.slice(..PLAIN_BLOCK_SIZE as usize)
        );
        assert!(input.read().is_err());
    }

    #[test]
    fn test_truncated_empty_file_is_rejected() {
        let fileio = FileIO::memory();
        let path = "memory:///test/truncated_empty.bin";
        let output = EncryptedOutputFile::new(
            fileio.new_output(path).unwrap(),
            key_metadata(),
        );
        let file_metadata = output.write(Bytes::new()).unwrap();
        let metadata = key_metadata().with_file_length(file_metadata.size);
        let ciphertext = fileio.new_input(path).unwrap().read().unwrap();
        fileio
            .new_output(path)
            .unwrap()
            .write(&ciphertext.slice(..GCM_STREAM_HEADER_LENGTH as usize))
            .unwrap();
        let input =
            EncryptedInputFile::new(fileio.new_input(path).unwrap(), metadata);
        assert!(input.read().is_err());
    }

    #[test]
    fn test_close_returns_encrypted_size() {
        let fileio = FileIO::memory();
        let path = "memory:///test/streaming.bin";
        let output = EncryptedOutputFile::new(
            fileio.new_output(path).unwrap(),
            key_metadata(),
        );
        for plaintext in [
            Bytes::from(vec![42; PLAIN_BLOCK_SIZE as usize + 17]),
            Bytes::new(),
        ] {
            let mut writer = output.writer().unwrap();
            for chunk in plaintext.chunks(1024) {
                writer.write_all(chunk).unwrap();
            }
            let metadata = writer.close().unwrap();
            let size = fileio.new_input(path).unwrap().metadata().unwrap().size;
            assert_eq!(metadata.size, size);
        }
    }
}
