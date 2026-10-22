#[test]
fn committed_typescript_matches_the_control_plane() {
    assert_eq!(
        include_str!("../frontend/components/ocg/contracts/generated.ts"),
        ocg::control_plane::contracts::typescript(),
        "Control Plane contracts have drifted; run make contracts"
    );
}
