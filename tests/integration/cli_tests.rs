//! CLI integration tests
//!
//! Tests for the sqlflight command-line interface.

use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use tempfile::TempDir;

fn sqlflight() -> Command {
    Command::cargo_bin("sqlflight").unwrap()
}

mod fmt_command {
    use super::*;

    #[test]
    fn fmt_single_file_to_stdout() {
        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("query.sql");
        fs::write(&file_path, "SELECT ID, NAME FROM USERS").unwrap();

        sqlflight()
            .arg("fmt")
            .arg(&file_path)
            .assert()
            .success()
            .stdout(predicate::str::contains("select id, name from users"));
    }

    #[test]
    fn fmt_single_file_in_place() {
        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("query.sql");
        fs::write(&file_path, "SELECT ID, NAME FROM USERS").unwrap();

        sqlflight()
            .arg("fmt")
            .arg("--write")
            .arg(&file_path)
            .assert()
            .success();

        let content = fs::read_to_string(&file_path).unwrap();
        assert!(content.contains("select"));
        assert!(!content.contains("SELECT"));
    }

    #[test]
    fn fmt_multiple_files() {
        let temp = TempDir::new().unwrap();
        let file1 = temp.path().join("query1.sql");
        let file2 = temp.path().join("query2.sql");
        fs::write(&file1, "SELECT A FROM B").unwrap();
        fs::write(&file2, "SELECT C FROM D").unwrap();

        sqlflight()
            .arg("fmt")
            .arg("--write")
            .arg(&file1)
            .arg(&file2)
            .assert()
            .success();

        assert!(fs::read_to_string(&file1).unwrap().contains("select"));
        assert!(fs::read_to_string(&file2).unwrap().contains("select"));
    }

    #[test]
    fn fmt_directory_recursive() {
        let temp = TempDir::new().unwrap();
        let subdir = temp.path().join("models");
        fs::create_dir(&subdir).unwrap();

        let file1 = temp.path().join("root.sql");
        let file2 = subdir.join("model.sql");
        fs::write(&file1, "SELECT A FROM B").unwrap();
        fs::write(&file2, "SELECT C FROM D").unwrap();

        sqlflight()
            .arg("fmt")
            .arg("--write")
            .arg(temp.path())
            .assert()
            .success();

        assert!(fs::read_to_string(&file1).unwrap().contains("select"));
        assert!(fs::read_to_string(&file2).unwrap().contains("select"));
    }

    #[test]
    fn fmt_glob_pattern() {
        let temp = TempDir::new().unwrap();
        let models = temp.path().join("models");
        fs::create_dir(&models).unwrap();

        let sql_file = models.join("query.sql");
        let txt_file = models.join("readme.txt");
        fs::write(&sql_file, "SELECT A FROM B").unwrap();
        fs::write(&txt_file, "NOT SQL").unwrap();

        sqlflight()
            .arg("fmt")
            .arg("--write")
            .arg(models.join("*.sql"))
            .assert()
            .success();

        // SQL file should be formatted
        assert!(fs::read_to_string(&sql_file).unwrap().contains("select"));
        // TXT file should be unchanged
        assert_eq!(fs::read_to_string(&txt_file).unwrap(), "NOT SQL");
    }

    #[test]
    fn fmt_from_stdin() {
        sqlflight()
            .arg("fmt")
            .arg("-")
            .write_stdin("SELECT A FROM B")
            .assert()
            .success()
            .stdout(predicate::str::contains("select a from b"));
    }

    #[test]
    fn fmt_parse_error_exits_with_code_2() {
        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("invalid.sql");
        fs::write(&file_path, "SELECT FROM WHERE").unwrap();

        sqlflight()
            .arg("fmt")
            .arg(&file_path)
            .assert()
            .code(2);
    }

    #[test]
    fn fmt_preserves_semicolon() {
        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("query.sql");
        fs::write(&file_path, "SELECT A FROM B;").unwrap();

        sqlflight()
            .arg("fmt")
            .arg(&file_path)
            .assert()
            .success()
            .stdout(predicate::str::contains(";"));
    }

    #[test]
    fn fmt_no_semicolon_stays_without() {
        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("query.sql");
        fs::write(&file_path, "SELECT A FROM B").unwrap();

        sqlflight()
            .arg("fmt")
            .arg(&file_path)
            .assert()
            .success()
            .stdout(predicate::str::is_match(r"select a from b\s*$").unwrap());
    }
}

mod check_command {
    use super::*;

    #[test]
    fn check_already_formatted_exits_0() {
        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("query.sql");
        fs::write(&file_path, "select id, name from users\n").unwrap();

        sqlflight()
            .arg("check")
            .arg(&file_path)
            .assert()
            .success();
    }

    #[test]
    fn check_needs_formatting_exits_1() {
        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("query.sql");
        fs::write(&file_path, "SELECT ID, NAME FROM USERS").unwrap();

        sqlflight()
            .arg("check")
            .arg(&file_path)
            .assert()
            .code(1);
    }

    #[test]
    fn check_parse_error_exits_2() {
        let temp = TempDir::new().unwrap();
        let file_path = temp.path().join("invalid.sql");
        fs::write(&file_path, "SELECT FROM WHERE").unwrap();

        sqlflight()
            .arg("check")
            .arg(&file_path)
            .assert()
            .code(2);
    }

    #[test]
    fn check_directory() {
        let temp = TempDir::new().unwrap();
        let file1 = temp.path().join("formatted.sql");
        let file2 = temp.path().join("unformatted.sql");

        // One formatted, one not
        fs::write(&file1, "select a from b\n").unwrap();
        fs::write(&file2, "SELECT A FROM B").unwrap();

        sqlflight()
            .arg("check")
            .arg(temp.path())
            .assert()
            .code(1);
    }

    #[test]
    fn check_all_formatted_directory_exits_0() {
        let temp = TempDir::new().unwrap();
        let file1 = temp.path().join("query1.sql");
        let file2 = temp.path().join("query2.sql");

        fs::write(&file1, "select a from b\n").unwrap();
        fs::write(&file2, "select c from d\n").unwrap();

        sqlflight()
            .arg("check")
            .arg(temp.path())
            .assert()
            .success();
    }
}

mod cli_options {
    use super::*;

    #[test]
    fn help_flag() {
        sqlflight()
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("sqlflight"))
            .stdout(predicate::str::contains("fmt"))
            .stdout(predicate::str::contains("check"));
    }

    #[test]
    fn version_flag() {
        sqlflight()
            .arg("--version")
            .assert()
            .success()
            .stdout(predicate::str::contains("sqlflight"));
    }

    #[test]
    fn fmt_help() {
        sqlflight()
            .arg("fmt")
            .arg("--help")
            .assert()
            .success()
            .stdout(predicate::str::contains("--write"));
    }

    #[test]
    fn check_help() {
        sqlflight()
            .arg("check")
            .arg("--help")
            .assert()
            .success();
    }

    #[test]
    fn missing_file_argument() {
        sqlflight()
            .arg("fmt")
            .assert()
            .failure();
    }

    #[test]
    fn nonexistent_file_error() {
        sqlflight()
            .arg("fmt")
            .arg("/nonexistent/path/file.sql")
            .assert()
            .failure();
    }
}
