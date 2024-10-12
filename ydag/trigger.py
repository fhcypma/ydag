from enum import Enum
from typing import List


class State(Enum):
    CREATED = 0
    WAITING = (1,)
    RUNNING = (2,)
    SKIPPED = (3,)
    SUCCEEDED = (4,)
    FAILED = (5,)
    UPSTREAM_FAILED = (6,)
    UPSTREAM_SKIPPED = (7,)

    def __str__(self):
        return self.name


NOT_DONE_STATES = {State.CREATED, State.WAITING, State.RUNNING}
DONE_STATES = {
    State.SKIPPED,
    State.UPSTREAM_SKIPPED,
    State.SUCCEEDED,
    State.FAILED,
    State.UPSTREAM_FAILED,
}
SKIPPED_STATES = {State.SKIPPED, State.UPSTREAM_SKIPPED}
FAILED_STATES = {State.FAILED, State.UPSTREAM_FAILED}


class Trigger(Enum):
    """
    all_success: Default rule requiring all upstream tasks to succeed. If any was skipped, skip.
    all_failed: Requires all upstream tasks to fail. If any was skipped, skip.
    all_done: All upstream tasks have completed, regardless of outcome.
    one_success: At least one upstream task has succeeded.
    one_failed: At least one upstream task has failed.
    none_failed: No upstream tasks have failed.
    none_skipped: No upstream tasks were skipped.
    """

    ALL_SUCCESS = "all_success"
    ALL_FAILED = "all_failed"
    ALL_DONE = "all_done"
    ONE_SUCCESS = "one_success"
    ONE_FAILED = "one_failed"
    NONE_FAILED = "none_failed"
    NONE_SKIPPED = "none_skipped"

    def next_state(self, predecessor_states: List[State]) -> State:
        if any([state in NOT_DONE_STATES for state in predecessor_states]):
            return State.WAITING

        if self == Trigger.ALL_SUCCESS:
            if set(predecessor_states) == {State.SUCCEEDED}:
                return State.RUNNING
            if any([state in SKIPPED_STATES for state in predecessor_states]):
                return State.UPSTREAM_SKIPPED
            if any([state in FAILED_STATES for state in predecessor_states]):
                return State.UPSTREAM_FAILED

        if self == Trigger.ALL_FAILED:
            if set(predecessor_states).issubset(FAILED_STATES):
                return State.RUNNING
            if any([state in SKIPPED_STATES for state in predecessor_states]):
                return State.UPSTREAM_SKIPPED
            if any([state in FAILED_STATES for state in predecessor_states]):
                return State.UPSTREAM_FAILED

        if self == Trigger.ALL_DONE:
            if set(predecessor_states).issubset(DONE_STATES):
                return State.RUNNING
            return State.FAILED

        if self == Trigger.ONE_SUCCESS:
            if any([state == State.SUCCEEDED for state in predecessor_states]):
                return State.RUNNING
            if not any([state in FAILED_STATES for state in predecessor_states]):
                return State.UPSTREAM_FAILED
            return State.FAILED

        if self == Trigger.ONE_FAILED:
            if not any([state in FAILED_STATES for state in predecessor_states]):
                return State.RUNNING
            return State.FAILED

        if self == Trigger.NONE_FAILED:
            if any([state in FAILED_STATES for state in predecessor_states]):
                return State.FAILED
            return State.RUNNING

        if self == Trigger.NONE_SKIPPED:
            if any([state in SKIPPED_STATES for state in predecessor_states]):
                return State.FAILED
            return State.RUNNING

        return State.WAITING
