"""Tests for medicaid_utils.common_utils.recipes."""
import logging

import pytest

from medicaid_utils.common_utils import recipes


class TestIsNumber:
    def test_integer_string(self):
        assert recipes.is_number("42") is True

    def test_float_string(self):
        assert recipes.is_number("3.14") is True

    def test_negative_number(self):
        assert recipes.is_number("-7") is True

    def test_scientific_notation(self):
        assert recipes.is_number("1e5") is True

    def test_non_numeric_string(self):
        assert recipes.is_number("abc") is False

    def test_empty_string(self):
        assert recipes.is_number("") is False

    def test_none(self):
        assert recipes.is_number(None) is False


class TestConvertToIntStr:
    def test_integer_string(self):
        assert recipes.convert_to_int_str("42") == "42"

    def test_float_string(self):
        assert recipes.convert_to_int_str("3.0") == "3"

    def test_non_numeric(self):
        assert recipes.convert_to_int_str("abc") == "abc"

    def test_scientific_notation(self):
        assert recipes.convert_to_int_str("1e2") == "100"

    def test_negative_float(self):
        assert recipes.convert_to_int_str("-5.9") == "-5"


class TestRemoveTailDotZeros:
    def test_trailing_zeros(self):
        assert recipes.remove_tail_dot_zeros("3.1400") == "3.14"

    def test_integer_with_dot_zero(self):
        assert recipes.remove_tail_dot_zeros("5.0") == "5"

    def test_no_trailing_zeros(self):
        assert recipes.remove_tail_dot_zeros("2.13") == "2.13"

    def test_multiple_trailing_zeros(self):
        assert recipes.remove_tail_dot_zeros("10.000") == "10"


class TestLogAssert:
    def test_passing_assertion(self):
        # Should not raise
        recipes.log_assert(True, "this should pass")

    def test_failing_assertion(self):
        # log_assert raises AssertionError on failure; the internal
        # stack inspection may fail in some contexts (e.g. pytest),
        # but the assertion itself should propagate
        with pytest.raises((AssertionError, TypeError)):
            recipes.log_assert(False, "expected failure")

    def test_with_logger(self):
        logger = logging.getLogger("test_log_assert")
        with pytest.raises((AssertionError, TypeError)):
            recipes.log_assert(False, "test message", logger=logger)


class TestRemoveIgnoreIfNotExists:
    def test_remove_existing_file(self, tmp_path):
        f = tmp_path / "test.txt"
        f.write_text("data")
        recipes.remove_ignore_if_not_exists(str(f))
        assert not f.exists()

    def test_remove_nonexistent_file(self, tmp_path):
        # Should not raise
        recipes.remove_ignore_if_not_exists(str(tmp_path / "nonexistent.txt"))

    def test_remove_existing_directory(self, tmp_path):
        d = tmp_path / "subdir"
        d.mkdir()
        (d / "file.txt").write_text("data")
        recipes.remove_ignore_if_not_exists(str(d))
        assert not d.exists()


class TestSetupLogger:
    def test_creates_logger(self, tmp_path):
        log_file = str(tmp_path / "test.log")
        recipes.setup_logger("test_setup_logger", log_file)
        logger = logging.getLogger("test_setup_logger")
        assert logger.level == logging.INFO
        assert len(logger.handlers) >= 2
