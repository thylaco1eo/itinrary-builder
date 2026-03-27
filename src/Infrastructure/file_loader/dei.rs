use std::fmt;

#[derive(Debug, PartialEq, Clone)]
pub enum Dei {
    // --- Specific Codes ---
    JointOperationAirlineDesignators,    // 001
    OperatingAirlineDisclosureCodeShare, // 002
    AircraftOwner,                       // 003
    CockpitCrewEmployer,                 // 004
    CabinCrewEmployer,                   // 005
    OnwardFlight,                        // 006
    MealServiceNote,                     // 007
    TrafficRestrictionNote,              // 008
    OpAirlineDisclosureSharedLease,      // 009
    DupLegCrossRefDupLegId,              // 010
    PartnershipSpecification,            // 011

    DupLegCrossRefOpsLegId, // 050

    UtcLocalTimeVariation, // 097
    PaxTerminalArr,        // 098
    PaxTerminalDep,        // 099

    PrbdSegmentOverride,    // 101 (Passenger Reservations Booking Designator)
    PrbmSegmentOverride,    // 102 (Modifier)
    BlockedSeatsOrUld,      // 104
    RestrictedPayload,      // 105
    PrbdExceedingMaxLength, // 106
    PrbmExceedingMaxLength, // 107
    AircraftConfigExceedingMaxLength, // 108
    MealServiceNoteExceedingMaxLength, // 109

    MealServiceSegmentOverride, // 111
    AircraftOwnerSpec,          // 113
    CockpitCrewEmployerSpec,    // 114
    CabinCrewEmployerSpec,      // 115

    AircraftTypePubOverride,    // 121
    FlightNumberOverride,       // 122
    JointOpAirlineSegOverride,  // 125
    OperatingAirlineDisclosure, // 127

    TrafficRestrPaxOnly,       // 170
    TrafficRestrCargoMailOnly, // 171
    TrafficRestrCargoOnly,     // 172
    TrafficRestrMailOnly,      // 173

    PaxTerminalSegOverrideArr, // 198
    PaxTerminalSegOverrideDep, // 199

    SubjectToGovApproval,         // 201
    PlaneChangeNoTypeChange,      // 210
    MinConnectTimeIntDomOverride, // 220

    PaxCheckIn,                    // 299
    FlaglandingOffPointOnly,       // 301
    FlaglandingOffPointBoardPoint, // 302
    FlaglandingBoardPointOnly,     // 303

    OnTimePerfIndicator,     // 501
    OnTimePerfDelayCancel,   // 502
    InFlightServiceInfo,     // 503
    SecureFlightIndicator,   // 504
    ElectronicTicketingInfo, // 505
    RequestAllReservations,  // 507

    TrafficRestrQualifierBoardPoint,     // 710
    TrafficRestrQualifierOffPoint,       // 711
    TrafficRestrQualifierBoardOffPoints, // 712

    // --- Ranges ---
    TrafficRestrFreeFormat(u16), // 713-799
    BilateralUse(u16),           // 800-899
    InternalUse(u16),            // 900-999

    // --- Fallback ---
    Unknown(u16),
}

impl Dei {
    /// 将字符串代码 (如 "050") 转换为 Enum
    pub fn from_code(code_str: &str) -> Self {
        let code = code_str.trim().parse::<u16>().unwrap_or(0);
        match code {
            1 => Dei::JointOperationAirlineDesignators,
            2 => Dei::OperatingAirlineDisclosureCodeShare,
            3 => Dei::AircraftOwner,
            4 => Dei::CockpitCrewEmployer,
            5 => Dei::CabinCrewEmployer,
            6 => Dei::OnwardFlight,
            7 => Dei::MealServiceNote,
            8 => Dei::TrafficRestrictionNote,
            9 => Dei::OpAirlineDisclosureSharedLease,
            10 => Dei::DupLegCrossRefDupLegId,
            11 => Dei::PartnershipSpecification,
            50 => Dei::DupLegCrossRefOpsLegId,
            97 => Dei::UtcLocalTimeVariation,
            98 => Dei::PaxTerminalArr,
            99 => Dei::PaxTerminalDep,
            101 => Dei::PrbdSegmentOverride,
            102 => Dei::PrbmSegmentOverride,
            104 => Dei::BlockedSeatsOrUld,
            105 => Dei::RestrictedPayload,
            106 => Dei::PrbdExceedingMaxLength,
            107 => Dei::PrbmExceedingMaxLength,
            108 => Dei::AircraftConfigExceedingMaxLength,
            109 => Dei::MealServiceNoteExceedingMaxLength,
            111 => Dei::MealServiceSegmentOverride,
            113 => Dei::AircraftOwnerSpec,
            114 => Dei::CockpitCrewEmployerSpec,
            115 => Dei::CabinCrewEmployerSpec,
            121 => Dei::AircraftTypePubOverride,
            122 => Dei::FlightNumberOverride,
            125 => Dei::JointOpAirlineSegOverride,
            127 => Dei::OperatingAirlineDisclosure,
            170 => Dei::TrafficRestrPaxOnly,
            171 => Dei::TrafficRestrCargoMailOnly,
            172 => Dei::TrafficRestrCargoOnly,
            173 => Dei::TrafficRestrMailOnly,
            198 => Dei::PaxTerminalSegOverrideArr,
            199 => Dei::PaxTerminalSegOverrideDep,
            201 => Dei::SubjectToGovApproval,
            210 => Dei::PlaneChangeNoTypeChange,
            220 => Dei::MinConnectTimeIntDomOverride,
            299 => Dei::PaxCheckIn,
            301 => Dei::FlaglandingOffPointOnly,
            302 => Dei::FlaglandingOffPointBoardPoint,
            303 => Dei::FlaglandingBoardPointOnly,
            501 => Dei::OnTimePerfIndicator,
            502 => Dei::OnTimePerfDelayCancel,
            503 => Dei::InFlightServiceInfo,
            504 => Dei::SecureFlightIndicator,
            505 => Dei::ElectronicTicketingInfo,
            507 => Dei::RequestAllReservations,
            710 => Dei::TrafficRestrQualifierBoardPoint,
            711 => Dei::TrafficRestrQualifierOffPoint,
            712 => Dei::TrafficRestrQualifierBoardOffPoints,
            713..=799 => Dei::TrafficRestrFreeFormat(code),
            800..=899 => Dei::BilateralUse(code),
            900..=999 => Dei::InternalUse(code),
            _ => Dei::Unknown(code),
        }
    }

    /// 获取人类可读的官方描述
    pub fn description(&self) -> &'static str {
        match self {
            Dei::JointOperationAirlineDesignators => "Joint Operation Airline Designators",
            Dei::OperatingAirlineDisclosureCodeShare => "Operating Airline Disclosure — Code Share",
            Dei::AircraftOwner => "Aircraft Owner",
            Dei::CockpitCrewEmployer => "Cockpit Crew Employer",
            Dei::CabinCrewEmployer => "Cabin Crew Employer",
            Dei::OnwardFlight => "Onward Flight",
            Dei::MealServiceNote => "Meal Service Note",
            Dei::TrafficRestrictionNote => "Traffic Restriction Note",
            Dei::OpAirlineDisclosureSharedLease => {
                "Operating Airline Disclosure — Shared Airline or Wet Lease"
            }
            Dei::DupLegCrossRefDupLegId => {
                "Duplicate Leg Cross Reference — Duplicate Leg Identification"
            }
            Dei::PartnershipSpecification => "Partnership Specification",
            Dei::DupLegCrossRefOpsLegId => {
                "Duplicate Leg Cross Reference — Operational Leg Identification"
            }
            Dei::UtcLocalTimeVariation => "UTC/Local Time Variation Specification",
            Dei::PaxTerminalArr => "Passenger Terminal Identifier — Arrival",
            Dei::PaxTerminalDep => "Passenger Terminal Identifier — Departure",
            Dei::PrbdSegmentOverride => {
                "Passenger Reservations Booking Designator Segment Override"
            }
            Dei::PrbmSegmentOverride => "Passenger Reservations Booking Modifier Segment Override",
            Dei::BlockedSeatsOrUld => "Blocked Seats and/or Unit Load Devices",
            Dei::RestrictedPayload => "Restricted Payload",
            Dei::PrbdExceedingMaxLength => "PRBD Exceeding Maximum Length",
            Dei::PrbmExceedingMaxLength => "PRBM Exceeding Maximum Length",
            Dei::AircraftConfigExceedingMaxLength => {
                "Aircraft Configuration/Version Exceeding Maximum Length"
            }
            Dei::MealServiceNoteExceedingMaxLength => "Meal Service Note Exceeding Maximum Length",
            Dei::MealServiceSegmentOverride => "Meal Service Segment Override",
            Dei::AircraftOwnerSpec => "Aircraft Owner Specification",
            Dei::CockpitCrewEmployerSpec => "Cockpit Crew Employer Specification",
            Dei::CabinCrewEmployerSpec => "Cabin Crew Employer Specification",
            Dei::AircraftTypePubOverride => "Aircraft Type Publication Override",
            Dei::FlightNumberOverride => "Flight Number Override",
            Dei::JointOpAirlineSegOverride => {
                "Joint Operation Airline Designators Segment Override"
            }
            Dei::OperatingAirlineDisclosure => "Operating Airline Disclosure",
            Dei::TrafficRestrPaxOnly => "Traffic Restriction Code — Passengers Only",
            Dei::TrafficRestrCargoMailOnly => "Traffic Restriction Code — Cargo/Mail Only",
            Dei::TrafficRestrCargoOnly => "Traffic Restriction Code — Cargo Only",
            Dei::TrafficRestrMailOnly => "Traffic Restriction Code — Mail Only",
            Dei::PaxTerminalSegOverrideArr => "Passenger Terminal Segment Override — Arrival",
            Dei::PaxTerminalSegOverrideDep => "Passenger Terminal Segment Override — Departure",
            Dei::SubjectToGovApproval => "Subject to Government Approval",
            Dei::PlaneChangeNoTypeChange => "Plane Change without Aircraft Type Change",
            Dei::MinConnectTimeIntDomOverride => "MCT International/Domestic Status Override",
            Dei::PaxCheckIn => "Passenger Check-In",
            Dei::FlaglandingOffPointOnly => "Flaglanding at Off Point Only",
            Dei::FlaglandingOffPointBoardPoint => "Flaglanding at Off Point and Board Point",
            Dei::FlaglandingBoardPointOnly => "Flaglanding at Board Point Only",
            Dei::OnTimePerfIndicator => "On-Time Performance Indicator",
            Dei::OnTimePerfDelayCancel => {
                "On-Time Performance Indicator for Delays & Cancellations"
            }
            Dei::InFlightServiceInfo => "In-Flight Service Information",
            Dei::SecureFlightIndicator => "Secure Flight Indicator",
            Dei::ElectronicTicketingInfo => "Electronic Ticketing Information",
            Dei::RequestAllReservations => "Request All Reservations",
            Dei::TrafficRestrQualifierBoardPoint => {
                "Traffic Restriction Code Qualifier at Board Point"
            }
            Dei::TrafficRestrQualifierOffPoint => "Traffic Restriction Code Qualifier at Off Point",
            Dei::TrafficRestrQualifierBoardOffPoints => {
                "Traffic Restriction Code Qualifier at Board and Off Points"
            }
            Dei::TrafficRestrFreeFormat(_) => "Traffic Restriction Code Information — Free Format",
            Dei::BilateralUse(_) => "Data Element Identifiers — Free Format Bilateral Use",
            Dei::InternalUse(_) => "Data Element Identifiers — Free Format Internal Use",
            Dei::Unknown(_) => "Unknown DEI",
        }
    }
}

// 为了方便打印
impl fmt::Display for Dei {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.description())
    }
}
