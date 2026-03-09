use std::io::{BufRead, BufReader};
use crate::Infrastructure::file_loader::oag_parser::*;

#[derive(Debug)]
pub struct FlightBlock {
    pub leg: FlightLegRecord,             // Type 3
    pub segments: Vec<SegmentDataRecord>, // Type 4 list
}

#[derive(Debug)]
pub enum ParseItem {
    Header(HeaderRecord),
    Season(SeasonRecord),
    Flight(FlightBlock), // <-- 聚合后的核心数据
    Trailer(TrailerRecord),
    Error(anyhow::Error),
}

pub struct OagStreamIterator<R> {
    reader: std::io::Lines<BufReader<R>>, // 底层的行读取器
    current_flight: Option<FlightBlock>,  // 状态缓冲：当前正在构建的航班
    queued_item: Option<ParseItem>,       // 临时缓冲：用于处理状态切换时的暂存项
}

impl<R: std::io::Read> OagStreamIterator<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader).lines(),
            current_flight: None,
            queued_item: None,
        }
    }

    // 辅助：将当前的 buffer 封装并清空
    fn flush_current_flight(&mut self) -> Option<ParseItem> {
        if let Some(block) = self.current_flight.take() {
            Some(ParseItem::Flight(block))
        } else {
            None
        }
    }
}

impl<R: std::io::Read> Iterator for OagStreamIterator<R> {
    type Item = ParseItem;

    fn next(&mut self) -> Option<Self::Item> {
        // 1. 如果有因为状态切换而暂存的非航班记录（比如 Header/Trailer），优先返回它
        if let Some(item) = self.queued_item.take() {
            return Some(item);
        }

        // 2. 循环读取，直到我们能够由足够的信息产出一个完整的 Item
        while let Some(line_result) = self.reader.next() {
            let line = match line_result {
                Ok(l) => l,
                Err(e) => return Some(ParseItem::Error(anyhow::Error::new(e))),
            };

            // 解析当前行
            // 注意：这里调用之前写的 OagParser::parse_line
            let record = match OagParser::parse_line(&line) {
                Ok(r) => r,
                Err(e) => return Some(ParseItem::Error(e)),
            };

            match record {
                // --- Case A: 遇到 Type 3 (新航班开始) ---
                OagRecord::FlightLeg(leg_record) => {
                    // 创建新航班的容器
                    let new_block = FlightBlock {
                        leg: leg_record,
                        segments: Vec::new(),
                    };

                    // 关键逻辑：如果之前已经有一个航班在缓存里，说明那个航班结束了
                    let result = if self.current_flight.is_some() {
                        // 1. 取出旧航班准备返回
                        let old_flight = self.flush_current_flight();
                        // 2. 保存新航班到 buffer
                        self.current_flight = Some(new_block);
                        // 3. 返回旧航班
                        old_flight
                    } else {
                        // 这是一个全新的开始（比如文件刚开头），存入 buffer，继续读下一行找 Type 4
                        self.current_flight = Some(new_block);
                        continue;
                    };

                    if result.is_some() { return result; }
                }

                // --- Case B: 遇到 Type 4 (子记录) ---
                OagRecord::SegmentData(seg_record) => {
                    if let Some(ref mut block) = self.current_flight {
                        // 业务校验：确保 Type 4 属于当前的 Type 3
                        // 严格模式下应该检查 airline/flight_number/seq 是否匹配
                        // 这里简单处理：直接追加
                        block.segments.push(seg_record);
                    } else {
                        // 错误情况：出现了 Type 4 但前面没有 Type 3
                        return Some(ParseItem::Error(anyhow::anyhow!("Orphan Type 4 record found")));
                    }
                    // Type 4 消耗掉了，继续读下一行
                    continue;
                }

                // --- Case C: 其他记录 (Header, Season, Trailer) ---
                OagRecord::Header(h) => return Some(ParseItem::Header(h)),
                OagRecord::Season(s) => {
                    // 如果 Season 出现，意味着之前的航班组可能结束了
                    // 但通常 Season 出现在文件头。
                    // 如果在中间出现，我们需要先 Flush 掉之前的航班
                    if self.current_flight.is_some() {
                        self.queued_item = Some(ParseItem::Season(s)); // 下一次返回这个
                        return self.flush_current_flight(); // 这一次返回航班
                    }
                    return Some(ParseItem::Season(s));
                }
                OagRecord::Trailer(t) => {
                    // Trailer 意味着当前 Season 数据块结束，必须 Flush 掉手头的航班
                    if self.current_flight.is_some() {
                        self.queued_item = Some(ParseItem::Trailer(t));
                        return self.flush_current_flight();
                    }
                    return Some(ParseItem::Trailer(t));
                }
                _ => continue, // 忽略 Unknown
            }
        }

        // 3. 文件读完了 (EOF)，如果 buffer 里还有最后一个航班，通过 flush 返回
        self.flush_current_flight()
    }
}