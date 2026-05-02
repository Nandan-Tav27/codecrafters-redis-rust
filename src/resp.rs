use bytes::{Bytes, BytesMut};
use memchr::memchr;
use tokio_util::codec::{Decoder, Encoder};

pub type Value = Bytes;
pub type Key = Bytes;

type RedisResult = Result<Option<(usize, RedisBufSplit)>, RESPError>;

#[derive(Default)]
pub struct RESPParser;

#[derive(PartialEq, Clone, Debug)]
pub enum RedisValueRef {
    SimpleString(Bytes),
    String(Bytes),
    Error(Bytes),
    Int(i64),
    Array(Vec<RedisValueRef>),
    NullArray,
    NullBulkString,
}

struct BufSplit(usize, usize);

impl BufSplit {
    #[inline]
    fn as_slice<'a>(&self, buf: &'a BytesMut) -> &'a [u8] {
        &buf[self.0..self.1]
    }

    #[inline]
    fn as_bytes(&self, buf: &Bytes) -> Bytes {
        buf.slice(self.0..self.1)
    }
}

enum RedisBufSplit {
    String(BufSplit),
    Error(BufSplit),
    Int(i64),
    Array(Vec<RedisBufSplit>),
    NullArray,
    NullBulkString,
}

impl RedisBufSplit {
    fn redis_value(self, buf: &Bytes) -> RedisValueRef {
        match self {
            RedisBufSplit::String(bfs) => RedisValueRef::String(bfs.as_bytes(buf)),
            RedisBufSplit::Error(bfs) => RedisValueRef::Error(bfs.as_bytes(buf)),
            RedisBufSplit::Array(arr) => {
                RedisValueRef::Array(arr.into_iter().map(|bfs| bfs.redis_value(buf)).collect())
            }
            RedisBufSplit::NullArray => RedisValueRef::NullArray,
            RedisBufSplit::NullBulkString => RedisValueRef::NullBulkString,
            RedisBufSplit::Int(i) => RedisValueRef::Int(i),
        }
    }
}

#[derive(Debug)]
pub enum RESPError {
    UnexpectedEnd,
    UnknownStartingByte,
    IOError(std::io::Error),
    IntParseFailure,
    BadBulkStringSize(i64),
    BadArraySize(i64),
}

impl From<std::io::Error> for RESPError {
    fn from(e: std::io::Error) -> RESPError {
        RESPError::IOError(e)
    }
}

/// Get a word from buf, starting at pos
#[inline]
fn word(buf: &BytesMut, pos: usize) -> Option<(usize, BufSplit)> {
    if buf.len() <= pos {
        return None;
    }

    memchr(b'\r', &buf[pos..]).and_then(|end| {
        if pos + end + 1 < buf.len() && buf[pos + end + 1] == b'\n' {
            Some((pos + end + 2, BufSplit(pos, pos + end)))
        } else {
            None
        }
    })
}

fn simple_string(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, RedisBufSplit::String(word))))
}

fn error(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(word(buf, pos).map(|(pos, word)| (pos, RedisBufSplit::Error(word))))
}

fn int(buf: &BytesMut, pos: usize) -> Result<Option<(usize, i64)>, RESPError> {
    match word(buf, pos) {
        Some((pos, word)) => {
            let s = str::from_utf8(word.as_slice(buf)).map_err(|_| RESPError::IntParseFailure)?;
            let i = s.parse().map_err(|_| RESPError::IntParseFailure)?;
            Ok(Some((pos, i)))
        }
        None => Ok(None),
    }
}

fn resp_int(buf: &BytesMut, pos: usize) -> RedisResult {
    Ok(int(buf, pos)?.map(|(pos, int)| (pos, RedisBufSplit::Int(int))))
}

fn bulk_string(buf: &BytesMut, pos: usize) -> RedisResult {
    match int(buf, pos)? {
        Some((pos, -1)) => Ok(Some((pos, RedisBufSplit::NullBulkString))),
        Some((pos, size)) if size >= 0 => {
            let total_size = pos + size as usize;
            if buf.len() < total_size + 2 {
                Ok(None)
            } else {
                let bb = RedisBufSplit::String(BufSplit(pos, total_size));
                Ok(Some((total_size + 2, bb)))
            }
        }
        Some((_pos, bad_size)) => Err(RESPError::BadBulkStringSize(bad_size)),
        None => Ok(None),
    }
}

fn array(buf: &BytesMut, pos: usize) -> RedisResult {
    match int(buf, pos)? {
        None => Ok(None),
        Some((pos, -1)) => Ok(Some((pos, RedisBufSplit::NullArray))),
        Some((pos, num_elements)) if num_elements >= 0 => {
            let mut values = Vec::with_capacity(num_elements as usize);
            let mut curr_pos = pos;
            for _ in 0..num_elements {
                match parse(buf, curr_pos)? {
                    Some((new_pos, value)) => {
                        curr_pos = new_pos;
                        values.push(value);
                    }
                    None => return Ok(None),
                }
            }
            Ok(Some((curr_pos, RedisBufSplit::Array(values))))
        }
        Some((_pos, bad_num_elements)) => Err(RESPError::BadArraySize(bad_num_elements)),
    }
}

fn parse(buf: &BytesMut, pos: usize) -> RedisResult {
    if buf.is_empty() {
        return Ok(None);
    }

    match buf[pos] {
        b'+' => simple_string(buf, pos + 1),
        b'-' => error(buf, pos + 1),
        b'$' => bulk_string(buf, pos + 1),
        b':' => resp_int(buf, pos + 1),
        b'*' => array(buf, pos + 1),
        _ => Err(RESPError::UnknownStartingByte),
    }
}

impl Decoder for RESPParser {
    type Item = RedisValueRef;
    type Error = RESPError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.is_empty() {
            return Ok(None);
        }

        match parse(buf, 0)? {
            Some((pos, value)) => {
                let data = buf.split_to(pos);
                Ok(Some(value.redis_value(&data.freeze())))
            }
            None => Ok(None),
        }
    }
}

impl Encoder<RedisValueRef> for RESPParser {
    type Error = std::io::Error;

    fn encode(&mut self, item: RedisValueRef, dst: &mut BytesMut) -> std::io::Result<()> {
        write_redis_value(item, dst);
        Ok(())
    }
}

fn write_redis_value(item: RedisValueRef, dst: &mut BytesMut) {
    match item {
        RedisValueRef::Error(e) => {
            dst.extend_from_slice(b"-");
            dst.extend_from_slice(&e);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::SimpleString(s) => {
            dst.extend_from_slice(b"+");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::String(s) => {
            dst.extend_from_slice(b"$");
            dst.extend_from_slice(s.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            dst.extend_from_slice(&s);
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::Array(arr) => {
            dst.extend_from_slice(b"*");
            dst.extend_from_slice(arr.len().to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
            for redis_value in arr {
                write_redis_value(redis_value, dst);
            }
        }
        RedisValueRef::Int(i) => {
            dst.extend_from_slice(b":");
            dst.extend_from_slice(i.to_string().as_bytes());
            dst.extend_from_slice(b"\r\n");
        }
        RedisValueRef::NullBulkString => dst.extend_from_slice(b"$-1\r\n"),
        RedisValueRef::NullArray => dst.extend_from_slice(b"*-1\r\n"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    // --- Decoder tests ---

    #[test]
    fn decode_simple_string() {
        let mut buf = BytesMut::from("+OK\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RedisValueRef::String(Bytes::from("OK")));
    }

    #[test]
    fn decode_error() {
        let mut buf = BytesMut::from("-ERR unknown command\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RedisValueRef::Error(Bytes::from("ERR unknown command"))
        );
    }

    #[test]
    fn decode_integer() {
        let mut buf = BytesMut::from(":1337\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RedisValueRef::Int(1337));
    }

    #[test]
    fn decode_negative_integer() {
        let mut buf = BytesMut::from(":-42\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RedisValueRef::Int(-42));
    }

    #[test]
    fn decode_bulk_string() {
        let mut buf = BytesMut::from("$5\r\nhello\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RedisValueRef::String(Bytes::from("hello")));
    }

    #[test]
    fn decode_empty_bulk_string() {
        let mut buf = BytesMut::from("$0\r\n\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RedisValueRef::String(Bytes::from("")));
    }

    #[test]
    fn decode_null_bulk_string() {
        let mut buf = BytesMut::from("$-1\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RedisValueRef::NullBulkString);
    }

    #[test]
    fn decode_null_array() {
        let mut buf = BytesMut::from("*-1\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RedisValueRef::NullArray);
    }

    #[test]
    fn decode_empty_array() {
        let mut buf = BytesMut::from("*0\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(result, RedisValueRef::Array(vec![]));
    }

    #[test]
    fn decode_array_of_bulk_strings() {
        let mut buf = BytesMut::from("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("hello")),
                RedisValueRef::String(Bytes::from("world")),
            ])
        );
    }

    #[test]
    fn decode_mixed_array() {
        let mut buf = BytesMut::from("*3\r\n:1\r\n$3\r\nfoo\r\n+bar\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::Int(1),
                RedisValueRef::String(Bytes::from("foo")),
                RedisValueRef::String(Bytes::from("bar")),
            ])
        );
    }

    #[test]
    fn decode_incomplete_returns_none() {
        let mut buf = BytesMut::from("$5\r\nhel");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn decode_empty_returns_none() {
        let mut buf = BytesMut::new();
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn decode_set_command() {
        let mut buf = BytesMut::from("*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
        let mut parser = RESPParser;
        let result = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(
            result,
            RedisValueRef::Array(vec![
                RedisValueRef::String(Bytes::from("SET")),
                RedisValueRef::String(Bytes::from("foo")),
                RedisValueRef::String(Bytes::from("bar")),
            ])
        );
    }

    #[test]
    fn decode_consumes_buffer() {
        let mut buf = BytesMut::from("+OK\r\n+NEXT\r\n");
        let mut parser = RESPParser;
        let first = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(first, RedisValueRef::String(Bytes::from("OK")));
        let second = parser.decode(&mut buf).unwrap().unwrap();
        assert_eq!(second, RedisValueRef::String(Bytes::from("NEXT")));
    }

    // --- Encoder tests ---

    #[test]
    fn encode_bulk_string() {
        let mut buf = BytesMut::new();
        let mut parser = RESPParser;
        parser
            .encode(RedisValueRef::String(Bytes::from("hello")), &mut buf)
            .unwrap();
        assert_eq!(buf, "$5\r\nhello\r\n");
    }

    #[test]
    fn encode_error() {
        let mut buf = BytesMut::new();
        let mut parser = RESPParser;
        parser
            .encode(RedisValueRef::Error(Bytes::from("ERR unknown")), &mut buf)
            .unwrap();
        assert_eq!(buf, "-ERR unknown\r\n");
    }

    #[test]
    fn encode_integer() {
        let mut buf = BytesMut::new();
        let mut parser = RESPParser;
        parser.encode(RedisValueRef::Int(42), &mut buf).unwrap();
        assert_eq!(buf, ":42\r\n");
    }

    #[test]
    fn encode_negative_integer() {
        let mut buf = BytesMut::new();
        let mut parser = RESPParser;
        parser.encode(RedisValueRef::Int(-100), &mut buf).unwrap();
        assert_eq!(buf, ":-100\r\n");
    }

    #[test]
    fn encode_null_bulk_string() {
        let mut buf = BytesMut::new();
        let mut parser = RESPParser;
        parser
            .encode(RedisValueRef::NullBulkString, &mut buf)
            .unwrap();
        assert_eq!(buf, "$-1\r\n");
    }

    #[test]
    fn encode_null_array() {
        let mut buf = BytesMut::new();
        let mut parser = RESPParser;
        parser.encode(RedisValueRef::NullArray, &mut buf).unwrap();
        assert_eq!(buf, "*-1\r\n");
    }

    #[test]
    fn encode_array() {
        let mut buf = BytesMut::new();
        let mut parser = RESPParser;
        parser
            .encode(
                RedisValueRef::Array(vec![
                    RedisValueRef::String(Bytes::from("SET")),
                    RedisValueRef::String(Bytes::from("foo")),
                    RedisValueRef::String(Bytes::from("bar")),
                ]),
                &mut buf,
            )
            .unwrap();
        assert_eq!(buf, "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n");
    }

    #[test]
    fn encode_empty_array() {
        let mut buf = BytesMut::new();
        let mut parser = RESPParser;
        parser
            .encode(RedisValueRef::Array(vec![]), &mut buf)
            .unwrap();
        assert_eq!(buf, "*0\r\n");
    }

    // --- Round-trip tests ---

    #[test]
    fn roundtrip_bulk_string() {
        let mut encoder = RESPParser;
        let mut decoder = RESPParser;
        let original = RedisValueRef::String(Bytes::from("hello"));

        let mut buf = BytesMut::new();
        encoder.encode(original.clone(), &mut buf).unwrap();
        let decoded = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, original);
    }

    #[test]
    fn roundtrip_array() {
        let mut encoder = RESPParser;
        let mut decoder = RESPParser;
        let original = RedisValueRef::Array(vec![
            RedisValueRef::String(Bytes::from("PING")),
            RedisValueRef::Int(42),
        ]);

        let mut buf = BytesMut::new();
        encoder.encode(original.clone(), &mut buf).unwrap();
        let decoded = decoder.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded, original);
    }
}
