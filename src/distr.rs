use rand::{seq::SliceRandom, Rng};
use rand_distr::{Distribution, Geometric, Standard, Zipf};

pub struct MyExp {
    distr: Geometric,
}

impl MyExp {
    pub fn new(lambda: f64) -> Self {
        Self {
            distr: Geometric::new(lambda).unwrap(),
        }
    }
}

impl Distribution<u32> for MyExp {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u32 {
        self.distr.sample(rng) as u32
    }
}

impl Distribution<(u32, u32)> for MyExp {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> (u32, u32) {
        (self.sample(rng), 0)
    }
}

impl Distribution<u64> for MyExp {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        self.distr.sample(rng)
    }
}

impl Distribution<(u64, u64)> for MyExp {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> (u64, u64) {
        (self.sample(rng), 0)
    }
}

pub struct KeyUniform;

impl Distribution<(u32, u32)> for KeyUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> (u32, u32) {
        (Standard.sample(rng), 0)
    }
}

impl Distribution<(u64, u64)> for KeyUniform {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> (u64, u64) {
        (Standard.sample(rng), 0)
    }
}

pub struct ZipfU32 {
    distr: Zipf<f32>,
}

impl ZipfU32 {
    pub fn new(s: f32) -> Self {
        Self {
            distr: Zipf::new(u32::MAX as u64, s).unwrap(),
        }
    }
}

impl Distribution<u32> for ZipfU32 {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u32 {
        self.distr.sample(rng) as u32
    }
}

impl Distribution<(u32, u32)> for ZipfU32 {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> (u32, u32) {
        (self.sample(rng), 0)
    }
}

pub struct StepUniformU32 {
    values: Vec<u32>,
}

impl StepUniformU32 {
    pub fn new(n: u32) -> Self {
        let s = (<u32>::MAX / (n + 1)) as usize;
        Self {
            values: (0..<u32>::MAX)
                .into_iter()
                .skip(s)
                .step_by(s)
                .take(n as usize)
                .collect(),
        }
    }
}

impl Distribution<u32> for StepUniformU32 {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u32 {
        *self.values.choose(rng).unwrap()
    }
}

impl Distribution<(u32, u32)> for StepUniformU32 {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> (u32, u32) {
        (self.sample(rng), 0)
    }
}

pub struct ZipfU64 {
    distr: Zipf<f32>,
}

impl ZipfU64 {
    pub fn new(s: f32) -> Self {
        Self {
            distr: Zipf::new(u64::MAX, s).unwrap(),
        }
    }
}

impl Distribution<u64> for ZipfU64 {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        self.distr.sample(rng) as u64
    }
}

impl Distribution<(u64, u64)> for ZipfU64 {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> (u64, u64) {
        (self.sample(rng), 0)
    }
}

pub struct StepUniformU64 {
    values: Vec<u64>,
}

impl StepUniformU64 {
    pub fn new(n: u64) -> Self {
        let s = (<u64>::MAX / (n + 1)) as usize;
        Self {
            values: (0..<u64>::MAX)
                .into_iter()
                .skip(s)
                .step_by(s)
                .take(n as usize)
                .collect(),
        }
    }
}

impl Distribution<u64> for StepUniformU64 {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> u64 {
        *self.values.choose(rng).unwrap()
    }
}

impl Distribution<(u64, u64)> for StepUniformU64 {
    fn sample<R: Rng + ?Sized>(&self, rng: &mut R) -> (u64, u64) {
        (self.sample(rng), 0)
    }
}
