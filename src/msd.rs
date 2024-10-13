use std::thread;

fn print_bytes(data: &[u8]) {
    for e in data {
        println!("{:08b}", e);
    }
    println!();
}

pub fn sort_parts(data: &[u8], bit: u8, name: &str) -> Vec<u8> {
    let name = format!("{}{}", name, bit);
    let r = if bit == 0 {
        data.iter()
            .filter_map(|e| (*e & 1u8 == 0).then_some(*e))
            .chain(data.iter().filter_map(|e| (*e & 1u8 != 0).then_some(*e)))
            .collect::<Vec<_>>()
    } else {
        let l = data
            .iter()
            .filter_map(|e| (*e & (1u8 << bit) == 0).then_some(*e))
            .collect::<Vec<_>>();
        let lthread = if l.len() > 0 {
            let lname = name.clone() + " L";
            Some(thread::spawn(move || sort_parts(&l, bit - 1, &lname)))
        } else {
            None
        };
        let r = data
            .iter()
            .filter_map(|e| (*e & (1u8 << bit) != 0).then_some(*e))
            .collect::<Vec<_>>();
        let rthread = if r.len() > 0 {
            let rname = name.clone() + " R";
            Some(thread::spawn(move || sort_parts(&r, bit - 1, &rname)))
        } else {
            None
        };
        let l_sorted = lthread.map(|t| t.join().unwrap());
        let r_sorted = rthread.map(|t| t.join().unwrap());
        match (l_sorted, r_sorted) {
            (None, None) => vec![],
            (None, Some(r)) => r,
            (Some(l), None) => l,
            (Some(l), Some(r)) => [l, r].concat(),
        }
    };
    // println!("{}", name);
    // print_bytes(&r);
    r
}
