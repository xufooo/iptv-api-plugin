import importlib.util
import json
import sys
import types
import unittest
from pathlib import Path


class DummyLogger:
    def info(self, *args, **kwargs):
        pass

    def warning(self, *args, **kwargs):
        pass

    def error(self, *args, **kwargs):
        pass


class FakeCrontabManager:
    def __init__(self):
        self.calls = []

    def get_or_create(self, **kwargs):
        self.calls.append(kwargs)
        return types.SimpleNamespace(id=len(self.calls), **kwargs), True


class FakeTaskQuerySet:
    def __init__(self, manager):
        self.manager = manager

    def exclude(self, **kwargs):
        self.manager.excludes.append(kwargs)
        return self

    def update(self, **kwargs):
        self.manager.filter_updates.append(kwargs)
        return 0


class FakePeriodicTaskManager:
    def __init__(self):
        self.calls = []
        self.filters = []
        self.excludes = []
        self.filter_updates = []

    def update_or_create(self, *, name, defaults):
        self.calls.append((name, defaults))
        return types.SimpleNamespace(name=name, **defaults), True

    def filter(self, **kwargs):
        self.filters.append(kwargs)
        return FakeTaskQuerySet(self)


def load_plugin(crontab_manager, task_manager):
    django = types.ModuleType("django")
    django_utils = types.ModuleType("django.utils")
    django_timezone = types.ModuleType("django.utils.timezone")
    django_db = types.ModuleType("django.db")
    django_db.transaction = types.SimpleNamespace(atomic=lambda: None)
    django_utils.timezone = django_timezone

    celery = types.ModuleType("celery")

    def shared_task(*args, **kwargs):
        def decorator(func):
            func.celery_task_name = kwargs.get("name")
            return func

        return decorator

    celery.shared_task = shared_task

    beat_models = types.ModuleType("django_celery_beat.models")
    beat_models.CrontabSchedule = types.SimpleNamespace(objects=crontab_manager)
    beat_models.PeriodicTask = types.SimpleNamespace(objects=task_manager)
    django_celery_beat = types.ModuleType("django_celery_beat")
    django_celery_beat.models = beat_models

    core_models = types.ModuleType("core.models")
    core_models.CoreSettings = types.SimpleNamespace(
        get_system_time_zone=lambda: "Asia/Shanghai"
    )
    core = types.ModuleType("core")
    core.models = core_models

    modules = {
        "django": django,
        "django.utils": django_utils,
        "django.utils.timezone": django_timezone,
        "django.db": django_db,
        "celery": celery,
        "django_celery_beat": django_celery_beat,
        "django_celery_beat.models": beat_models,
        "core": core,
        "core.models": core_models,
    }

    module_path = Path(__file__).resolve().parents[1] / "iptv-api-plugin" / "plugin.py"
    module_name = "iptv_api_plugin_under_test"
    sys.modules.pop(module_name, None)
    sys.modules.update(modules)
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ScheduleTests(unittest.TestCase):
    def test_sync_schedule_creates_one_registered_task_per_time(self):
        crontab_manager = FakeCrontabManager()
        task_manager = FakePeriodicTaskManager()
        module = load_plugin(crontab_manager, task_manager)

        result = module.Plugin()._sync_schedule(
            {"schedule_times": "0600,1330"}, DummyLogger()
        )

        self.assertEqual(result["status"], "ok")
        self.assertEqual(
            crontab_manager.calls,
            [
                {
                    "minute": "0",
                    "hour": "6",
                    "day_of_week": "*",
                    "day_of_month": "*",
                    "month_of_year": "*",
                    "timezone": "Asia/Shanghai",
                },
                {
                    "minute": "30",
                    "hour": "13",
                    "day_of_week": "*",
                    "day_of_month": "*",
                    "month_of_year": "*",
                    "timezone": "Asia/Shanghai",
                },
            ],
        )
        self.assertEqual(
            [name for name, _ in task_manager.calls],
            [
                "iptv-api-plugin scheduled run 0600",
                "iptv-api-plugin scheduled run 1330",
            ],
        )
        self.assertTrue(
            all(
                defaults["task"] == module.SCHEDULED_TASK_PATH
                for _, defaults in task_manager.calls
            )
        )
        self.assertTrue(
            all(json.loads(defaults["args"]) == [] for _, defaults in task_manager.calls)
        )

    def test_empty_schedule_disables_current_and_legacy_tasks(self):
        crontab_manager = FakeCrontabManager()
        task_manager = FakePeriodicTaskManager()
        module = load_plugin(crontab_manager, task_manager)

        result = module.Plugin()._sync_schedule({"schedule_times": ""}, DummyLogger())

        self.assertEqual(
            result, {"status": "ok", "message": "Schedule disabled (times empty)."}
        )
        self.assertEqual(
            task_manager.filters,
            [{"name__startswith": "iptv-api-plugin scheduled run"}],
        )
        self.assertEqual(task_manager.filter_updates, [{"enabled": False}])


if __name__ == "__main__":
    unittest.main()
