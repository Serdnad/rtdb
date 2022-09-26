
fn to_table(data: &QueryResult) -> String {
    let mut s = String::from("│ ");
    s.push_str("timestamp                │ ");
    s.push_str(&data.records.fields.iter().map(|f| f.name.to_owned()).collect::<Vec<_>>().join("   │ "));
    s.push_str(" │\n");

    let header_len = s.len();
    s.push_str(&format!("├{}┤\n│", &"-".repeat(header_len)));
    // s.push_str(&"-".repeat(header_len));
    // s.push_str(&format!("{:_^8}"));

    for row in &data.records.rows[0..20] {
        let now = chrono::NaiveDateTime::from_timestamp(row.time / 1e9 as i64, 0); // TODO: nsecs
        let dt: DateTime<Utc> = DateTime::from_utc(now, Utc);

        s.push_str(&dt.to_rfc3339());
        s.push_str(" │  ");

        for (i, &elem) in row.elements.iter().enumerate() {
            let val_s = match elem {
                None => String::from(""),
                Some(val) => val.to_string()
            };

            let field = &data.records.fields[i];
            let _len = &data.records.fields[i].name.len();
            match field.data_type {
                DataType::Float => s.push_str(&format!("{: >7} │", val_s)),
                DataType::Bool => s.push_str(&format!("{: <7} │", val_s)),
            };
        }
        s.push_str("\n│");
    }

    s.push_str(&format!("{}/{}", data.count, data.count));

    s
}