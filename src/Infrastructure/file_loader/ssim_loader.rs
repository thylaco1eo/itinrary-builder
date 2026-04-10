use crate::Infrastructure::file_loader::oag_parser::*;
use std::collections::VecDeque;
use std::io::{BufRead, BufReader};

#[derive(Debug)]
pub struct FlightLegBlock {
    pub leg: FlightLegRecord,             // Type 3
    pub segments: Vec<SegmentDataRecord>, // Type 4 list
}

#[derive(Debug)]
pub struct FlightBlock {
    pub legs: Vec<FlightLegBlock>,
}

#[derive(Debug)]
pub enum ParseItem {
    Header(HeaderRecord),
    Season(SeasonRecord),
    Flight(FlightBlock), // One flight number / IVI with one or more contiguous legs.
    Trailer(TrailerRecord),
    Error(anyhow::Error),
}

pub struct OagStreamIterator<R> {
    reader: std::io::Lines<BufReader<R>>,
    current_leg: Option<FlightLegBlock>,
    current_flight: Option<FlightBlock>,
    queued_items: VecDeque<ParseItem>,
}

impl<R: std::io::Read> OagStreamIterator<R> {
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader).lines(),
            current_leg: None,
            current_flight: None,
            queued_items: VecDeque::new(),
        }
    }

    fn queue_current_flight(&mut self) {
        if let Some(block) = self.current_flight.take() {
            self.queued_items.push_back(ParseItem::Flight(block));
        }
    }

    fn complete_current_leg(&mut self) {
        let Some(leg_block) = self.current_leg.take() else {
            return;
        };

        if let Some(current_flight) = &mut self.current_flight {
            let continues_current_flight = current_flight
                .legs
                .last()
                .map(|previous| legs_belong_to_same_flight(&previous.leg, &leg_block.leg))
                .unwrap_or(false);

            if continues_current_flight {
                current_flight.legs.push(leg_block);
                return;
            }
        }

        self.queue_current_flight();
        self.current_flight = Some(FlightBlock {
            legs: vec![leg_block],
        });
    }

    fn queue_boundary_item(&mut self, item: ParseItem) {
        self.complete_current_leg();
        self.queue_current_flight();
        self.queued_items.push_back(item);
    }
}

impl<R: std::io::Read> Iterator for OagStreamIterator<R> {
    type Item = ParseItem;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(item) = self.queued_items.pop_front() {
            return Some(item);
        }

        while let Some(line_result) = self.reader.next() {
            let line = match line_result {
                Ok(l) => l,
                Err(e) => return Some(ParseItem::Error(anyhow::Error::new(e))),
            };

            let record = match OagParser::parse_line(&line) {
                Ok(r) => r,
                Err(e) => return Some(ParseItem::Error(e)),
            };

            match record {
                OagRecord::FlightLeg(leg_record) => {
                    self.complete_current_leg();
                    self.current_leg = Some(FlightLegBlock {
                        leg: leg_record,
                        segments: Vec::new(),
                    });
                    if let Some(item) = self.queued_items.pop_front() {
                        return Some(item);
                    }
                    continue;
                }

                OagRecord::SegmentData(seg_record) => {
                    if let Some(ref mut block) = self.current_leg {
                        block.segments.push(seg_record);
                    } else {
                        return Some(ParseItem::Error(anyhow::anyhow!(
                            "Orphan Type 4 record found"
                        )));
                    }
                    continue;
                }

                OagRecord::Header(h) => {
                    self.queue_boundary_item(ParseItem::Header(h));
                    return self.queued_items.pop_front();
                }
                OagRecord::Season(s) => {
                    self.queue_boundary_item(ParseItem::Season(s));
                    return self.queued_items.pop_front();
                }
                OagRecord::Trailer(t) => {
                    self.queue_boundary_item(ParseItem::Trailer(t));
                    return self.queued_items.pop_front();
                }
                _ => continue,
            }
        }

        self.complete_current_leg();
        self.queue_current_flight();
        self.queued_items.pop_front()
    }
}

fn legs_belong_to_same_flight(previous: &FlightLegRecord, next: &FlightLegRecord) -> bool {
    previous.airline_designator == next.airline_designator
        && previous.flight_number == next.flight_number
        && previous.itinerary_variation == next.itinerary_variation
        && previous.service_type == next.service_type
        && previous.frequency_rate == next.frequency_rate
        && next.leg_sequence == previous.leg_sequence.saturating_add(1)
        && previous.arrival_station == next.departure_station
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn groups_contiguous_legs_with_same_flight_number_into_one_block() {
        let data = concat!(
            "3 CA  8650101J04FEB2604MAR26  3     PEK07000700+08003 MAD12001200+01001 789XX                       XX                 II                                                   C30W34Y229          00006098\n",
            "4 CA  8650101J              AB010PEKMADLA 8783                                                                                                                                                    006099\n",
            "3 CA  8650102J04FEB2604MAR26  3     MAD14001400+01001 HAV18151815-05003 789XX                       XX                 II                                                   C30W34Y229          00006104\n",
            "4 CA  8650102J              BC106MADHAVJCDZRGEYBMUHQVWSTLPNK                                                                                                                                      006105\n",
        );
        let mut iterator = OagStreamIterator::new(Cursor::new(data));

        let flight = iterator.next().expect("expected flight block");
        match flight {
            ParseItem::Flight(block) => {
                assert_eq!(block.legs.len(), 2);
                assert_eq!(block.legs[0].leg.departure_station, "PEK");
                assert_eq!(block.legs[0].leg.arrival_station, "MAD");
                assert_eq!(block.legs[1].leg.departure_station, "MAD");
                assert_eq!(block.legs[1].leg.arrival_station, "HAV");
                assert_eq!(block.legs[0].segments.len(), 1);
                assert_eq!(block.legs[1].segments.len(), 1);
            }
            other => panic!("expected flight block, got {other:?}"),
        }

        assert!(iterator.next().is_none());
    }
}
