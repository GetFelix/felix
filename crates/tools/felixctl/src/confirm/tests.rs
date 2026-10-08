use super::*;
use crate::error::exit_for;

const YES: Confirm = Confirm { yes: true };
const NO: Confirm = Confirm { yes: false };

#[test]
fn yes_skips_the_question() {
    confirm_with("Delete?", YES, false, || panic!("asked")).expect("confirmed");
}

#[test]
fn without_a_terminal_it_needs_yes() {
    let err = confirm_with("Delete?", NO, false, || panic!("asked")).unwrap_err();
    assert_eq!(exit_for(&err), Exit::Usage);
    assert!(err.to_string().contains("--yes"), "{err}");
}

#[test]
fn only_y_or_yes_confirms() {
    for answer in ["y\n", "YES\n", " yes "] {
        confirm_with("Delete?", NO, true, || Ok(answer.into())).expect(answer);
    }
    for answer in ["\n", "n\n", "yep\n"] {
        let err = confirm_with("Delete?", NO, true, || Ok(answer.into())).unwrap_err();
        assert_eq!(exit_for(&err), Exit::Failure, "{answer:?}");
    }
}
