fn main() {
    vnpkt::coder::generate_file_by_file(
        "./src/proto.pkt",
        vnpkt::coder::Language::Rust {
            tokio: true,
            tnl: false,
        },
        "./src/proto.rs",
    )
    .unwrap();
}