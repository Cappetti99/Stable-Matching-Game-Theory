from dataclasses import dataclass, field

@dataclass
class Task:
    id: int
    size: float
    assigned_vm: int | None = None

@dataclass
class VM:
    id: int
    capacity: float
    available_time: float = 0.0
    waiting_tasks: list = field(default_factory=list)
    threshold: int = 2  # max number of tasks per VM

def execution_time(task: Task, vm: VM) -> float:
    return task.size / vm.capacity