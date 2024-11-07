from ydag.deprecated.it1.dag import State, Trigger


class TestTrigger:
    class TestAllSuccess:
        def test_all_success(self):
            assert (
                Trigger.ALL_SUCCESS.next_state([State.SUCCEEDED, State.SUCCEEDED])
                == State.RUNNING
            )

        def test_one_waiting(self):
            assert (
                Trigger.ALL_SUCCESS.next_state([State.SUCCEEDED, State.CREATED])
                == State.WAITING
            )
            assert (
                Trigger.ALL_SUCCESS.next_state([State.SUCCEEDED, State.WAITING])
                == State.WAITING
            )
            assert (
                Trigger.ALL_SUCCESS.next_state([State.SUCCEEDED, State.RUNNING])
                == State.WAITING
            )

        def test_one_skipped(self):
            assert (
                Trigger.ALL_SUCCESS.next_state([State.SUCCEEDED, State.SKIPPED])
                == State.UPSTREAM_SKIPPED
            )
            assert (
                Trigger.ALL_SUCCESS.next_state(
                    [State.SUCCEEDED, State.UPSTREAM_SKIPPED]
                )
                == State.UPSTREAM_SKIPPED
            )

        def test_one_failed(self):
            assert (
                Trigger.ALL_SUCCESS.next_state([State.SUCCEEDED, State.FAILED])
                == State.UPSTREAM_FAILED
            )
            assert (
                Trigger.ALL_SUCCESS.next_state([State.SUCCEEDED, State.UPSTREAM_FAILED])
                == State.UPSTREAM_FAILED
            )

    class TestAllFailed:
        def test_all_failed(self):
            assert (
                Trigger.ALL_FAILED.next_state([State.FAILED, State.FAILED])
                == State.RUNNING
            )
            assert (
                Trigger.ALL_FAILED.next_state([State.FAILED, State.UPSTREAM_FAILED])
                == State.RUNNING
            )

        def test_one_waiting(self):
            assert (
                Trigger.ALL_FAILED.next_state([State.FAILED, State.CREATED])
                == State.WAITING
            )
            assert (
                Trigger.ALL_FAILED.next_state([State.FAILED, State.WAITING])
                == State.WAITING
            )
            assert (
                Trigger.ALL_FAILED.next_state([State.FAILED, State.RUNNING])
                == State.WAITING
            )

        def test_one_skipped(self):
            assert (
                Trigger.ALL_FAILED.next_state([State.FAILED, State.SKIPPED])
                == State.UPSTREAM_SKIPPED
            )
            assert (
                Trigger.ALL_FAILED.next_state([State.FAILED, State.UPSTREAM_SKIPPED])
                == State.UPSTREAM_SKIPPED
            )

        def test_one_failed(self):
            assert (
                Trigger.ALL_FAILED.next_state([State.SUCCEEDED, State.FAILED])
                == State.UPSTREAM_FAILED
            )
            assert (
                Trigger.ALL_FAILED.next_state([State.SUCCEEDED, State.UPSTREAM_FAILED])
                == State.UPSTREAM_FAILED
            )
