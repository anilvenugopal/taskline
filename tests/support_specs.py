from taskline import TaskSpec

ECHO = TaskSpec(name="echo", queue="work")
SLOW = TaskSpec(name="slow", queue="work")
GRACED = TaskSpec(name="graced", queue="work", cancel_grace_seconds=600)
