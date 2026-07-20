"""RequestsLauncher 阻塞取结果路径单测：注入伪 pool，不起 Ray 集群。"""

import pytest

from llmperf.requests_launcher import RequestsLauncher


class _FakePool:
    """模拟 ray.util.ActorPool 的取结果接口。

    has_next() 语义与 ActorPool 一致：只看有没有在途 future，不看是否就绪；
    空池时 get_next_unordered() 抛 StopIteration。
    """

    def __init__(self, pending):
        self.pending = list(pending)
        self.has_next_calls = 0

    def has_next(self):
        self.has_next_calls += 1
        if self.has_next_calls > 1000:
            raise RuntimeError("busy-wait spin detected: has_next polled 1000+ times")
        return bool(self.pending)

    def get_next_unordered(self):
        if not self.pending:
            raise StopIteration("No more results to get")
        return self.pending.pop(0)


def _launcher_with(pool):
    launcher = RequestsLauncher.__new__(RequestsLauncher)
    launcher._llm_client_pool = pool
    return launcher


def test_block_on_empty_pool_raises_instead_of_spinning_forever():
    # M2：旧实现 `while not has_next(): pass` 在空池时热自旋到永远——
    # 没有在途请求就永远等不来结果。应快速失败（StopIteration）。
    pool = _FakePool(pending=[])
    launcher = _launcher_with(pool)
    with pytest.raises(StopIteration):
        launcher.get_next_ready(block=True)
    assert pool.has_next_calls <= 1000  # 未触发自旋检测


def test_block_returns_all_pending_results():
    launcher = _launcher_with(_FakePool(pending=["r1", "r2"]))
    assert launcher.get_next_ready(block=True) == ["r1", "r2"]


def test_non_block_drains_ready_results():
    launcher = _launcher_with(_FakePool(pending=["r1"]))
    assert launcher.get_next_ready() == ["r1"]
    assert launcher.get_next_ready() == []
