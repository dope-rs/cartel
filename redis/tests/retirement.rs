use std::{env, io::Write, process::Stdio};

use dope_test::checks::{Error, Suite};

#[test]
fn retirement_keeps_owner() -> Result<(), Error> {
    let exe = env::current_exe()?;
    let suite = Suite::new(
        env!("CARGO_MANIFEST_DIR"),
        file!(),
        &["cartel-redis", "dope", "o3"],
    );
    let status = suite.try_build(&exe, env!("CARGO_PKG_NAME"))?.status()?;
    assert!(status.success());
    for (source, errors, expected) in [
        (include_str!("common/retired.rs"), 0, &[] as &[&str]),
        (
            include_str!("common/retire.rs"),
            3,
            &[
                "self.0.disconnect(ctx);",
                "let proof: turns::Drained<'d, KeyTag<0>, N> = ctx.into_drained();",
                "let proof: conn::Retired<'d, KeyTag<0>, A> = proof;",
            ],
        ),
    ] {
        let mut compiler = suite
            .try_compiler(&exe)?
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()?;
        compiler
            .stdin
            .take()
            .expect("compiler stdin")
            .write_all(source.as_bytes())?;
        let output = compiler.wait_with_output()?;
        let diag = String::from_utf8(output.stderr).expect("diagnostic UTF-8");
        assert!(
            output.status.success() == (errors == 0)
                && diag.matches("error[E0308]: mismatched types").count() == errors
                && expected.iter().all(|part| diag.contains(part)),
            "{source}\n{diag}"
        );
    }
    Ok(())
}
