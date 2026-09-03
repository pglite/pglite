use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::Cursor;

pub const PAGE_SIZE: usize = 4096;
pub const PAGE_HEADER_SIZE: usize = 12;

#[derive(Debug, Clone)]
pub struct SlottedPage {
    pub page_id: u32,
    pub item_count: u16,
    pub lower: u16, // Points to end of line pointers (starts at 12)
    pub upper: u16, // Points to start of newest item (starts at PAGE_SIZE)
    pub data: [u8; PAGE_SIZE],
}

impl Default for SlottedPage {
    fn default() -> Self {
        let mut page = Self {
            page_id: 0,
            item_count: 0,
            lower: PAGE_HEADER_SIZE as u16,
            upper: PAGE_SIZE as u16,
            data: [0u8; PAGE_SIZE],
        };
        page.write_header();
        page
    }
}

impl SlottedPage {
    pub fn new(page_id: u32) -> Self {
        let mut p = Self::default();
        p.page_id = page_id;
        p.write_header();
        p
    }

    pub fn write_header(&mut self) {
        let mut cur = Cursor::new(&mut self.data[0..PAGE_HEADER_SIZE]);
        cur.write_u32::<LittleEndian>(self.page_id).unwrap();
        cur.write_u16::<LittleEndian>(self.item_count).unwrap();
        cur.write_u16::<LittleEndian>(self.lower).unwrap();
        cur.write_u16::<LittleEndian>(self.upper).unwrap();
    }

    pub fn read_header(&mut self) {
        let mut cur = Cursor::new(&self.data[0..PAGE_HEADER_SIZE]);
        self.page_id = cur.read_u32::<LittleEndian>().unwrap_or(0);
        self.item_count = cur.read_u16::<LittleEndian>().unwrap_or(0);
        self.lower = cur.read_u16::<LittleEndian>().unwrap_or(PAGE_HEADER_SIZE as u16);
        self.upper = cur.read_u16::<LittleEndian>().unwrap_or(PAGE_SIZE as u16);
    }

    pub fn free_space(&self) -> usize {
        if self.upper >= self.lower + 4 {
            (self.upper - self.lower - 4) as usize
        } else {
            0
        }
    }

    pub fn insert_tuple(&mut self, tuple_data: &[u8]) -> Option<u16> {
        let required = tuple_data.len() + 4; // 4 bytes for line pointer (offset: u16, len: u16)
        if (self.upper as usize) < (self.lower as usize) + required {
            return None; // Page is full
        }

        let new_upper = self.upper - tuple_data.len() as u16;
        self.data[new_upper as usize..(new_upper as usize + tuple_data.len())]
            .copy_from_slice(tuple_data);

        let lp_offset = self.lower as usize;
        let mut cur = Cursor::new(&mut self.data[lp_offset..lp_offset + 4]);
        cur.write_u16::<LittleEndian>(new_upper).unwrap();
        cur.write_u16::<LittleEndian>(tuple_data.len() as u16).unwrap();

        let index = self.item_count;
        self.item_count += 1;
        self.lower += 4;
        self.upper = new_upper;
        self.write_header();

        Some(index)
    }

    pub fn get_tuple(&self, item_index: u16) -> Option<&[u8]> {
        if item_index >= self.item_count {
            return None;
        }
        let lp_offset = PAGE_HEADER_SIZE + (item_index as usize * 4);
        let mut cur = Cursor::new(&self.data[lp_offset..lp_offset + 4]);
        let offset = cur.read_u16::<LittleEndian>().ok()? as usize;
        let len = cur.read_u16::<LittleEndian>().ok()? as usize;

        if offset == 0 || len == 0 || offset + len > PAGE_SIZE {
            return None;
        }
        Some(&self.data[offset..offset + len])
    }
}
