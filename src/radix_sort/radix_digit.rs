//TODO: should restrict to Unpin?
pub trait RadixDigit: Send + Sync {
    const DIGITS: u8;

    fn get_digit(&self, digit: u8) -> u8;
}

impl RadixDigit for u8 {
    const DIGITS: u8 = 1;

    fn get_digit(&self, digit: u8) -> u8 {
        (*self >> digit * 8) as u8
    }
}

impl RadixDigit for u16 {
    const DIGITS: u8 = 2;

    fn get_digit(&self, digit: u8) -> u8 {
        (*self >> digit * 8) as u8
    }
}

impl RadixDigit for u32 {
    const DIGITS: u8 = 4;

    fn get_digit(&self, digit: u8) -> u8 {
        (*self >> digit * 8) as u8
    }
}

impl RadixDigit for u64 {
    const DIGITS: u8 = 8;

    fn get_digit(&self, digit: u8) -> u8 {
        (*self >> digit * 8) as u8
    }
}

impl RadixDigit for u128 {
    const DIGITS: u8 = 16;

    fn get_digit(&self, digit: u8) -> u8 {
        (*self >> digit * 8) as u8
    }
}

impl RadixDigit for usize {
    const DIGITS: u8 = size_of::<usize>() as u8;

    fn get_digit(&self, digit: u8) -> u8 {
        (*self >> digit * 8) as u8
    }
}

impl RadixDigit for i8 {
    const DIGITS: u8 = 1;

    fn get_digit(&self, digit: u8) -> u8 {
        ((*self ^ i8::MIN) >> digit * 8) as u8
    }
}

impl RadixDigit for i16 {
    const DIGITS: u8 = 2;

    fn get_digit(&self, digit: u8) -> u8 {
        ((*self ^ i16::MIN) >> digit * 8) as u8
    }
}

impl RadixDigit for i32 {
    const DIGITS: u8 = 4;

    fn get_digit(&self, digit: u8) -> u8 {
        ((*self ^ i32::MIN) >> digit * 8) as u8
    }
}

impl RadixDigit for i64 {
    const DIGITS: u8 = 8;

    fn get_digit(&self, digit: u8) -> u8 {
        ((*self ^ i64::MIN) >> digit * 8) as u8
    }
}

impl RadixDigit for i128 {
    const DIGITS: u8 = 16;

    fn get_digit(&self, digit: u8) -> u8 {
        ((*self ^ i128::MIN) >> digit * 8) as u8
    }
}

impl RadixDigit for isize {
    const DIGITS: u8 = size_of::<isize>() as u8;

    fn get_digit(&self, digit: u8) -> u8 {
        ((*self ^ isize::MIN) >> digit * 8) as u8
    }
}

// http://stereopsis.com/radix.html
// - on negative value flip all the bits
// - on positive value flip just the sign bit
impl RadixDigit for f32 {
    const DIGITS: u8 = 4;

    fn get_digit(&self, digit: u8) -> u8 {
        let mut b = self.to_bits() as i32;
        b ^= (b >> 31) | i32::MIN;
        (b as u32 >> digit * 8) as u8
    }
}

impl RadixDigit for f64 {
    const DIGITS: u8 = 8;

    fn get_digit(&self, digit: u8) -> u8 {
        let mut b = self.to_bits() as i64;
        b ^= (b >> 63) | i64::MIN;
        (b as u64 >> digit * 8) as u8
    }
}
