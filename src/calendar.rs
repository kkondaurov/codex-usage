use chrono::{DateTime, Duration, Local, LocalResult, NaiveDate, SecondsFormat, TimeZone, Utc};

pub(crate) fn canonical_utc_timestamp(value: DateTime<Utc>) -> String {
    value.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

pub(crate) fn local_midnight(date: NaiveDate) -> DateTime<Utc> {
    let midnight = date.and_hms_opt(0, 0, 0).expect("midnight is valid");
    (0..=48 * 60)
        .find_map(|offset_minutes| {
            let candidate = midnight + Duration::minutes(offset_minutes);
            match Local.from_local_datetime(&candidate) {
                LocalResult::Single(value) => Some(value.with_timezone(&Utc)),
                LocalResult::Ambiguous(first, _) => Some(first.with_timezone(&Utc)),
                LocalResult::None => None,
            }
        })
        .expect("the local timezone must contain an instant within 48 hours of a civil midnight")
}

#[cfg(test)]
mod tests {
    use super::{canonical_utc_timestamp, local_midnight};
    use chrono::{DateTime, Local, NaiveDate, Timelike, Utc};

    #[test]
    fn canonical_utc_timestamp_uses_nanosecond_utc_shape() {
        let timestamp = DateTime::parse_from_rfc3339("2026-07-25T12:34:56.123+02:00")
            .unwrap()
            .with_timezone(&Utc);

        assert_eq!(
            canonical_utc_timestamp(timestamp),
            "2026-07-25T10:34:56.123000000Z"
        );
    }

    #[test]
    fn local_midnight_returns_the_first_representable_local_instant() {
        let date = NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
        let local = local_midnight(date).with_timezone(&Local);

        assert!(local.date_naive() >= date);
        if local.date_naive() == date {
            assert_eq!(local.minute(), 0);
            assert_eq!(local.second(), 0);
        }
    }
}
