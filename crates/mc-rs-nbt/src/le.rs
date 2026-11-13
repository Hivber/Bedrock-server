//! Standard little-endian NBT variant (used for disk storage and chunk data).

use bytes::{Buf, BufMut};

use crate::error::NbtError;
use crate::io::NbtVariant;

pub(crate) struct LeVariant;

impl NbtVariant for LeVariant {
    fn write_int(buf: &mut impl BufMut, value: i32) {
        buf.put_i32_le(value);
    }

    fn read_int(buf: &mut impl Buf) -> Result<i32, NbtError> {
        if buf.remaining() < 4 {
            return Err(NbtError::UnexpectedEof);
        }
        Ok(buf.get_i32_le())
    }

    fn write_array_len(buf: &mut impl BufMut, len: i32) {
        buf.put_i32_le(len);
    }

    fn read_array_len(buf: &mut impl Buf) -> Result<i32, NbtError> {
        if buf.remaining() < 4 {
            return Err(NbtError::UnexpectedEof);
        }
        Ok(buf.get_i32_le())
    }

    fn write_string_len(buf: &mut impl BufMut, len: usize) {
        buf.put_u16_le(len as u16);
    }

    fn read_string_len(buf: &mut impl Buf) -> Result<usize, NbtError> {
        if buf.remaining() < 2 {
            return Err(NbtError::UnexpectedEof);
        }
        Ok(buf.get_u16_le() as usize)
    }

    fn read_short(buf: &mut impl Buf) -> Result<i16, NbtError> {
        if buf.remaining() < 2 {
            return Err(NbtError::UnexpectedEof);
        }
        Ok(buf.get_i16_le())
    }
    fn write_short(buf: &mut impl BufMut, value: i16) {
        buf.put_i16_le(value);
    }
    fn read_long(buf: &mut impl Buf) -> Result<i64, NbtError> {
        if buf.remaining() < 8 {
            return Err(NbtError::UnexpectedEof);
        }
        Ok(buf.get_i64_le())
    }
    fn write_long(buf: &mut impl BufMut, value: i64) {
        buf.put_i64_le(value);
    }
    fn read_float(buf: &mut impl Buf) -> Result<f32, NbtError> {
        if buf.remaining() < 4 {
            return Err(NbtError::UnexpectedEof);
        }
        Ok(buf.get_f32_le())
    }
    fn write_float(buf: &mut impl BufMut, value: f32) {
        buf.put_f32_le(value);
    }
    fn read_double(buf: &mut impl Buf) -> Result<f64, NbtError> {
        if buf.remaining() < 8 {
            return Err(NbtError::UnexpectedEof);
        }
        Ok(buf.get_f64_le())
    }
    fn write_double(buf: &mut impl BufMut, value: f64) {
        buf.put_f64_le(value);
    }
}
