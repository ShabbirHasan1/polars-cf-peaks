use rayon::prelude::*;
use std::time::Instant;

fn main() {
    let fruits = vec![
        b"Apple".to_vec(),
        b"Banana".to_vec(),
        b"Cherry".to_vec(),
        b"Date".to_vec(),
        b"Elderberry".to_vec(),
        b"Fig".to_vec(),
        b"Grape".to_vec(),
        b"Honeydew".to_vec(),
        b"Icaco".to_vec(),
        b"Jackfruit".to_vec(),
    ];

    let start1 = Instant::now();

    (0..10).into_par_iter().for_each(|_| {
        let mut buffer1 = Vec::new();
        for _ in 0..10_000_000 { 
            for fruit in &fruits {             
                buffer1.extend_from_slice(fruit);              
            }            
        }
        println!("{} ", buffer1.len());
    });

    let elapsed1 = start1.elapsed();

    println!("Append fruit: {:?}", elapsed1);

    let start2 = Instant::now();

    (0..10).into_par_iter().for_each(|_| {
        let mut buffer2 = Vec::new();
        for _ in 0..10_000_000 {
            for fruit in &fruits {

                if String::from_utf8_lossy(fruit) == "Apple" {
                    buffer2.extend_from_slice(fruit);
                }                              
            }            
        }
        println!("{} ", buffer2.len());
    });

    let elapsed2 = start2.elapsed();

    println!("Append fruit when apple exist: {:?}", elapsed2);
   
}


 /*
    if fruit == &b"Apple".to_vec() {
        buffer.extend_from_slice(fruit);
    } 
    
        match String::from_utf8_lossy(fruit).as_ref() {
        "Apple" => buffer.extend_from_slice(fruit),
        _ => (),
    }
    */
