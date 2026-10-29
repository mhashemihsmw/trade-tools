import pytest

from tests.conftest import ensure_isolated_test_database


def test_database_safety_guard_rejects_production_urls_without_disclosing_them():
    fake_url = "postgresql://test-user:test-password@host.test/db"
    with pytest.raises(pytest.UsageError) as configured_error:
        ensure_isolated_test_database(fake_url, "sqlite:///:memory:", fake_url)
    with pytest.raises(pytest.UsageError) as test_error:
        ensure_isolated_test_database(None, fake_url, fake_url)
    assert fake_url not in str(configured_error.value)
    assert fake_url not in str(test_error.value)
