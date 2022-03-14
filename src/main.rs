use std::io::BufReader;
use std::io::prelude::*;
use std::fs::File;
use std::env;
use std::vec::Vec;
use serde_json::{Deserializer, Value};
use serde::{Deserialize, Serialize};
use snailquote::{escape, unescape};

const RPI: u32 = 30; // Rows Per Insert
const OSL: usize = 250;	// object size limit in bytes (must be smaller than buf cap defined above)
			// and must be larger than largest json'ed object
const BUFCAP: usize = 300; // buf cap of 30 bytes
const TBL: &str = "tblName";

#[derive(Serialize, Deserialize, Debug)]
struct AnObject {
       id: u8,
       name: String,
       desc: String,
}

impl AnObject {
    fn full_ins(self, tbln: &str) -> String {
        format!("INSERT INTO `{}` (`id`, `name`, `desc`) VALUES({}, {}, {});",
            tbln, self.id, escape(&self.name),
            escape(&self.desc))
    }

    fn value_ins(self, tbln: &str) -> (String, String) {
        (format!("INSERT INTO `{}` (`id`, `name`, `desc`) VALUES ", tbln),
        format!("({}, {}, {})", self.id, escape(&self.name), 
                escape(&self.desc)))
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let file = File::open(&args[1]).expect("could not open file");
	let mut reader = BufReader::with_capacity(BUFCAP, file); 
    let mut parsed = 0;
    let mut iri = 1u32;
	let mut leftover = String::new();

	loop {
    	let block = String::from_utf8(reader.fill_buf()
					.unwrap().to_vec()).unwrap_or("??".to_string());
		if block.len() == 0 && leftover.len() == 0 { break; }
        leftover = parsechunk(leftover + &block, parsed, &mut iri);
        reader.consume(BUFCAP);
        parsed += 1;
    }
    if iri < RPI {
        print!(";");
    }
    println!("\n");
}
// rii: current row index in multirow SQL INSERT
// buckee: string to search for parseable JSON objects
// parsed: how many times parsechunk() has been called
fn parsechunk(buckee: String, parsed: u32, rii: &mut u32) -> String {
    let bucke = buckee.into_bytes();
    let mut bfirst: usize = 0;
    let mut consumed: usize = 0;

    if parsed == 0 { bfirst = 1 as usize; consumed += 1; }
    let bucket = &bucke[bfirst..bucke.len()];
    let mut ab = (0, 0);

    loop { 
        if ab.1 == 0 {
            ab = get_obj_byte_range(&bucket, 0);
        } else {
            if ab.1 < bucket.len() {
                ab = get_obj_byte_range(&bucket, ab.1 + 1);
            }
            else { break; }
        }
        if ab.1 - ab.0 > 0 {
            let mut endab = ab.1;

            if ab.1 > bucket.len() { 
                endab = bucket.len();
	        }
            consumed += endab - ab.0;
            let de = Deserializer::from_slice(&bucket[ab.0..endab]);
		//	let valiter = de.into_iter::<Value>();
			let valiter = de.into_iter::<AnObject>();

            for k in valiter {
                match k {
	            	Ok(f) => {
                        let (pi, ai) = f.value_ins(TBL);

                        if *rii == 1u32 {
                            print!("{}\n{}", pi, ai);
                        } else {
                            print!(",\n{}", ai);
                        }
                        *rii = *rii + 1u32;

                        if *rii > RPI {
                            *rii = 1u32;
                            print!(";\n");
                        }
                    },
	            	_ => (),
	  	        }
	        }
        }
		else { break; }
    }

	return std::str::from_utf8(&bucket[consumed..]).unwrap_or("??").to_string();
}

// starting from start return start and end end positions in
// supplied s string slice where a compelete JSON
// object can be found (in {})
fn get_obj_byte_range(s: &[u8], start: usize) -> (usize, usize) {
	let mut retstart = start;
	let mut startbraces = 0;
	let mut startfound = false;
	let mut inquote = false;
	let mut i = start;
	let mut sc = s[start..s.len()].iter();
    let mut pc = '.';

	loop {
        let c = match sc.next() {
            Some(v) => char::from(v.to_owned()),
            None => { break (retstart, i); },
        };
        
		if !startfound {
			if c == '{' {
				retstart = i;
				startbraces = 1;
				startfound = true;
			}
		} else {
			if !inquote {
				if c == '{' {
					startbraces += 1;
				}
				if c == '}' {
					startbraces -= 1;
				}
				if c == '"' {
					inquote = true;
				}
			} else {
				if c == '"' && pc != '\\' {
					inquote = false;
				}
			}
		}
        i += 1;

		if startfound && startbraces == 0 {
			return (retstart, i);
		}
		if i > OSL { 
            break (OSL, OSL); 
        }
        pc = c;
	}
}   
