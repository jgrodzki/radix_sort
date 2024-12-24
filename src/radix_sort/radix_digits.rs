pub trait RadixDigits: Send + Sync {
    const NUMBER_OF_DIGITS: u8;

    fn get_digit(&self, index: u8) -> u8;
}

impl RadixDigits for u8 {
    const NUMBER_OF_DIGITS: u8 = 1;

    fn get_digit(&self, index: u8) -> u8 {
        (*self >> index * 8) as u8
    }
}

impl RadixDigits for u16 {
    const NUMBER_OF_DIGITS: u8 = 2;

    fn get_digit(&self, index: u8) -> u8 {
        (*self >> index * 8) as u8
    }
}

impl RadixDigits for u32 {
    const NUMBER_OF_DIGITS: u8 = 4;

    fn get_digit(&self, index: u8) -> u8 {
        (*self >> index * 8) as u8
    }
}

impl RadixDigits for u64 {
    const NUMBER_OF_DIGITS: u8 = 8;

    fn get_digit(&self, index: u8) -> u8 {
        (*self >> index * 8) as u8
    }
}

impl RadixDigits for u128 {
    const NUMBER_OF_DIGITS: u8 = 16;

    fn get_digit(&self, index: u8) -> u8 {
        (*self >> index * 8) as u8
    }
}

impl RadixDigits for usize {
    const NUMBER_OF_DIGITS: u8 = size_of::<usize>() as u8;

    fn get_digit(&self, index: u8) -> u8 {
        (*self >> index * 8) as u8
    }
}

impl RadixDigits for i8 {
    const NUMBER_OF_DIGITS: u8 = 1;

    fn get_digit(&self, index: u8) -> u8 {
        ((*self ^ i8::MIN) >> index * 8) as u8
    }
}

impl RadixDigits for i16 {
    const NUMBER_OF_DIGITS: u8 = 2;

    fn get_digit(&self, index: u8) -> u8 {
        ((*self ^ i16::MIN) >> index * 8) as u8
    }
}

impl RadixDigits for i32 {
    const NUMBER_OF_DIGITS: u8 = 4;

    fn get_digit(&self, index: u8) -> u8 {
        ((*self ^ i32::MIN) >> index * 8) as u8
    }
}

impl RadixDigits for i64 {
    const NUMBER_OF_DIGITS: u8 = 8;

    fn get_digit(&self, index: u8) -> u8 {
        ((*self ^ i64::MIN) >> index * 8) as u8
    }
}

impl RadixDigits for i128 {
    const NUMBER_OF_DIGITS: u8 = 16;

    fn get_digit(&self, index: u8) -> u8 {
        ((*self ^ i128::MIN) >> index * 8) as u8
    }
}

impl RadixDigits for isize {
    const NUMBER_OF_DIGITS: u8 = size_of::<isize>() as u8;

    fn get_digit(&self, index: u8) -> u8 {
        ((*self ^ isize::MIN) >> index * 8) as u8
    }
}

// http://stereopsis.com/radix.html
// - on negative value flip all the bits
// - on positive value flip just the sign bit
impl RadixDigits for f32 {
    const NUMBER_OF_DIGITS: u8 = 4;

    fn get_digit(&self, index: u8) -> u8 {
        let mut b = self.to_bits() as i32;
        b ^= (b >> 31) | i32::MIN;
        (b as u32 >> index * 8) as u8
    }
}

impl RadixDigits for f64 {
    const NUMBER_OF_DIGITS: u8 = 8;

    fn get_digit(&self, index: u8) -> u8 {
        let mut b = self.to_bits() as i64;
        b ^= (b >> 63) | i64::MIN;
        (b as u64 >> index * 8) as u8
    }
}
