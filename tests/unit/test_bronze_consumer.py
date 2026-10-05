from bronze.consumer import should_flush


def test_bronze_batch_waits_for_size_or_deadline() -> None:
    assert not should_flush(pending_count=50, batch_size=100, now=9.0, deadline=10.0, once=False)
    assert should_flush(pending_count=100, batch_size=100, now=9.0, deadline=10.0, once=False)
    assert should_flush(pending_count=50, batch_size=100, now=10.0, deadline=10.0, once=False)


def test_bronze_once_flushes_nonempty_poll_only() -> None:
    assert should_flush(pending_count=1, batch_size=100, now=1.0, deadline=10.0, once=True)
    assert not should_flush(pending_count=0, batch_size=100, now=10.0, deadline=10.0, once=True)
