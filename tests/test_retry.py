"""Tests for retry logic."""

import pytest

from compliance_extractor.errors import QueryTimeoutError
from compliance_extractor.retry import RetryConfig, execute_with_retry, retry_on_timeout


class TestRetryConfig:
    def test_default_delays(self):
        config = RetryConfig()
        assert config.get_delay(1) == 1.0
        assert config.get_delay(2) == 2.0
        assert config.get_delay(3) == 4.0

    def test_custom_config(self):
        config = RetryConfig(base_delay_seconds=0.5, backoff_multiplier=3.0)
        assert config.get_delay(1) == 0.5
        assert config.get_delay(2) == 1.5
        assert config.get_delay(3) == 4.5


class TestRetryOnTimeout:
    def test_succeeds_first_try(self):
        call_count = 0

        @retry_on_timeout(max_retries=3, base_delay_seconds=0.01)
        def succeed():
            nonlocal call_count
            call_count += 1
            return "ok"

        assert succeed() == "ok"
        assert call_count == 1

    def test_retries_on_timeout(self):
        call_count = 0

        @retry_on_timeout(max_retries=3, base_delay_seconds=0.01)
        def fail_then_succeed():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise QueryTimeoutError("timeout")
            return "ok"

        assert fail_then_succeed() == "ok"
        assert call_count == 3

    def test_exhausts_retries(self):
        @retry_on_timeout(max_retries=2, base_delay_seconds=0.01)
        def always_fail():
            raise QueryTimeoutError("timeout")

        with pytest.raises(QueryTimeoutError):
            always_fail()

    def test_non_timeout_errors_propagate(self):
        @retry_on_timeout(max_retries=3, base_delay_seconds=0.01)
        def raise_value_error():
            raise ValueError("not a timeout")

        with pytest.raises(ValueError, match="not a timeout"):
            raise_value_error()


class TestExecuteWithRetry:
    def test_functional_retry(self):
        call_count = 0

        def flaky():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise QueryTimeoutError("timeout")
            return "ok"

        config = RetryConfig(max_retries=3, base_delay_seconds=0.01)
        assert execute_with_retry(flaky, config=config) == "ok"
        assert call_count == 2
