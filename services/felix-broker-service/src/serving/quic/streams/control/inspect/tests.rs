use super::page_size;

#[test]
fn a_page_is_never_empty_nor_unbounded() {
    assert_eq!(page_size(None), 100);
    assert_eq!(page_size(Some(0)), 1);
    assert_eq!(page_size(Some(250)), 250);
    assert_eq!(page_size(Some(u32::MAX)), 1000);
}
