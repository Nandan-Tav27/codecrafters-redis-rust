pub enum Types {
    SimpleString,
    SimpleError,
    Integetr,
    BulkString,
    Array,
}

pub struct Parser<'a> {
    remaining: &'a [u8],
}

impl<'a> Parser<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { remaining: bytes }
    }

    fn next(&mut self) -> Option<u8> {
        if self.remaining.is_empty() {
            None
        } else {
            let item = self.remaining[0];
            self.remaining = &self.remaining[1..];
            Some(item)
        }
    }

    fn next_n(&mut self, n: usize) -> Option<&[u8]> {
        if self.remaining.len() < n {
            None
        } else {
            let item = &self.remaining[0..n];
            self.remaining = &self.remaining[n..];
            Some(item)
        }
    }

    // Get next bite without consuming
    fn peek(&self) -> Option<u8> {
        if self.remaining.is_empty() {
            None
        } else {
            Some(self.remaining[0])
        }
    }

    fn consume_crlf(&mut self) -> Result<(), &'static str> {
        if self.remaining.starts_with(b"\r\n") {
            self.remaining = &self.remaining[2..];
            Ok(())
        } else {
            Err("could not consume crlf")
        }
    }

    fn consume_until_crlf(&mut self) -> Option<&[u8]> {
        match self.remaining.windows(2).position(|w| w == b"\r\n") {
            Some(pos) => {
                let item = &self.remaining[..pos];
                self.remaining = &self.remaining[pos..];
                Some(item)
            }
            None => None,
        }
    }

    fn is_empty(&self) -> bool {
        self.remaining.is_empty()
    }

    pub fn decode_simple_string(&mut self) -> Result<Option<String>, &'static str> {
        // parse +
        if self.next() != Some(b'+') {
            return Err("invalid resp simple string: should begin with '+'");
        }

        // parse string
        if let Some(str) = self.consume_until_crlf() {
            let data = String::from_utf8(str.to_vec())
                .map_err(|_| "invalid resp simple string: expected UTF8 string")?;

            // parse crlf
            self.consume_crlf()?;

            // return simple string
            Ok(Some(data))
        } else {
            Err("invalid resp simple string")
        }
    }

    pub fn decode_simple_error(&mut self) -> Result<Option<String>, &'static str> {
        // parse -
        if self.next() != Some(b'-') {
            return Err("invalid resp simple string: should begin with '-'");
        }

        // parse error string
        if let Some(str) = self.consume_until_crlf() {
            let data = String::from_utf8(str.to_vec())
                .map_err(|_| "invalid resp error string: expected UTF8 string")?;

            // parse crlf
            self.consume_crlf()?;

            // return simple error string
            Ok(Some(data))
        } else {
            Err("invalid resp error string")
        }
    }

    pub fn decode_integer(&mut self) -> Result<Option<i64>, &'static str> {
        // parse :
        if self.next() != Some(b':') {
            return Err("invalid resp integer: should begin with ':'");
        }

        let negative = self.peek() == Some(b'-');
        if negative {
            self.next();
        }

        let Some(byte) = self.next() else {
            return Err("invalid resp integer: exptected digit");
        };
        if !byte.is_ascii_digit() {
            return Err("invalid resp integer: exptected digit");
        }

        let mut val = (byte - b'0') as i64;
        while let Some(next_byte) = self.peek() {
            if next_byte.is_ascii_digit() {
                val = val * 10 + ((next_byte - b'0') as i64);
                self.next();
            } else {
                break;
            }
        }

        if negative {
            val = -val;
        }

        self.consume_crlf()?;

        Ok(Some(val))
    }

    pub fn decode_bulk_string(&mut self) -> Result<Option<String>, &'static str> {
        let mut data_len;

        // parse $
        if self.next() != Some(b'$') {
            return Err("invalid resp bulk string: should begin with '$'");
        }

        // parse length
        if let Some(byte) = self.next() {
            // null bulk string
            if byte == b'-' {
                if self.next() != Some(b'1') {
                    return Err("invalid resp bulk string");
                }
                self.consume_crlf()?;
                return Ok(None);
            } else {
                if !byte.is_ascii_digit() {
                    return Err("invalid resp bulk string");
                }
                data_len = (byte - b'0') as usize;
                while let Some(next_byte) = self.peek() {
                    if next_byte.is_ascii_digit() {
                        data_len = data_len * 10 + ((next_byte - b'0') as usize);
                        self.next();
                    } else {
                        break;
                    }
                }
            }
        } else {
            return Err("invalid resp bulk string");
        };

        // parse crlf
        self.consume_crlf()?;

        // parse data
        let Some(data_bytes) = self.next_n(data_len) else {
            return Err("invalid resp bulk string: invalid data field");
        };
        let data = String::from_utf8(data_bytes.to_vec())
            .map_err(|_| "invalid resp bulk string: expected UTF8 string")?;

        // parse crlf
        self.consume_crlf()?;

        // return data
        Ok(Some(data))
    }

    // *<number-of-elements>\r\n<element-1>...<element-n>
    pub fn decode_array(&mut self) -> Result<Option<Vec<String>>, &'static str> {
        let mut num_elements;

        // parse *
        if self.next() != Some(b'*') {
            return Err("invalid resp array: should begin with '*'");
        }

        // parse number of elements
        if let Some(byte) = self.next()
            && byte.is_ascii_digit()
        {
            num_elements = (byte - b'0') as usize;
            while let Some(next_byte) = self.peek() {
                if next_byte.is_ascii_digit() {
                    num_elements = num_elements * 10 + ((next_byte - b'0') as usize);
                    self.next();
                } else {
                    break;
                }
            }
        } else {
            return Err("invalid resp array: expected number of elements");
        };

        // parse crlf
        self.consume_crlf()?;

        let mut elements = Vec::new();
        // TODO: arrays currently only contain bulk strings
        for _ in 0..num_elements {
            match self.decode_bulk_string() {
                Ok(Some(el)) => elements.push(el),
                Ok(None) => elements.push("".to_string()),
                Err(e) => return Err(e),
            }
        }

        Ok(Some(elements))
    }
}

pub struct Encoder<'a> {
    buf: &'a mut [u8],
    ptr: usize,
}

impl<'a> Encoder<'a> {
    pub fn new(buf: &'a mut [u8]) -> Self {
        Self { buf, ptr: 0 }
    }

    fn set_byte(&mut self, val: u8) -> bool {
        if self.ptr < self.buf.len() {
            self.buf[self.ptr] = val;
            self.ptr += 1;
            true
        } else {
            false
        }
    }

    fn set_bytes(&mut self, val: &[u8]) -> bool {
        if self.ptr + val.len() <= self.buf.len() {
            self.buf[self.ptr..self.ptr + val.len()].copy_from_slice(val);
            self.ptr += val.len();
            true
        } else {
            false
        }
    }

    fn set_crlf(&mut self) -> bool {
        self.set_bytes(b"\r\n")
    }

    fn get_ptr(&self) -> usize {
        self.ptr
    }

    pub fn encode_to_bulk_string(&mut self, str: &str) -> Result<usize, &'static str> {
        let size_limit = 1024;
        let len = str.len();
        if len > size_limit {
            return Err("input size exceeds limit, cannot encode to bulk string");
        }

        if !self.set_byte(b'$') {
            return Err("buffer is not big enough");
        }

        if !self.set_bytes(len.to_string().as_bytes()) {
            return Err("buffer is not big enough");
        }

        if !self.set_crlf() {
            return Err("buffer is not big enough");
        }

        if !self.set_bytes(str.as_bytes()) {
            return Err("buffer is not big enough");
        }

        if !self.set_crlf() {
            return Err("buffer is not big enough");
        }

        Ok(self.get_ptr())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decode_bulk_str() {
        let bulk_str = b"$5\r\nhello\r\n";
        let mut bulk_str_parser = Parser::new(bulk_str);
        assert_eq!(
            bulk_str_parser.decode_bulk_string(),
            Ok(Some("hello".to_string()))
        );

        let bulk_str = b"$10\r\nhelloworld\r\n";
        let mut bulk_str_parser = Parser::new(bulk_str);
        assert_eq!(
            bulk_str_parser.decode_bulk_string(),
            Ok(Some("helloworld".to_string()))
        );

        let null_str = b"$-1\r\n";
        let mut null_str_parser = Parser::new(null_str);
        assert_eq!(null_str_parser.decode_bulk_string(), Ok(None));

        let err_str = b"5\r\nhello\r\n";
        let mut err_str_parser = Parser::new(err_str);
        assert_eq!(
            err_str_parser.decode_bulk_string(),
            Err("invalid resp bulk string: should begin with '$'")
        );
    }

    #[test]
    fn decode_arr() {
        let resp_arr = b"*0\r\n";
        let mut resp_arr_parser = Parser::new(resp_arr);
        assert_eq!(resp_arr_parser.decode_array(), Ok(Some(vec![])));

        let resp_arr = b"*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n";
        let mut resp_arr_parser = Parser::new(resp_arr);
        assert_eq!(
            resp_arr_parser.decode_array(),
            Ok(Some(vec!["hello".to_string(), "world".to_string()]))
        );
    }

    #[test]
    fn encode_to_bulk_str() {
        let str = "hello";
        let mut res_buf = [0; 1024];
        let mut encoder = Encoder::new(&mut res_buf);

        assert_eq!(encoder.encode_to_bulk_string(str), Ok(11));
        assert_eq!(res_buf[..11], *b"$5\r\nhello\r\n");
    }
}
