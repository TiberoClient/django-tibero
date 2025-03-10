import shutil

from django.db.backends.base.client import BaseDatabaseClient


class DatabaseClient(BaseDatabaseClient):
    executable_name = "tbsql"
    wrapper_name = "rlwrap"

    @staticmethod
    def connect_string(settings_dict):
        from .utils import dsn

        return '%s/"%s"@%s' % (
            settings_dict["USER"],
            settings_dict["PASSWORD"],
            dsn(settings_dict),
        )

    @classmethod
    def settings_to_cmd_args_env(cls, settings_dict, parameters):
        args = [cls.executable_name, "-L", cls.connect_string(settings_dict)]
        wrapper_path = shutil.which(cls.wrapper_name)
        if wrapper_path:
            args = [wrapper_path, *args]
        args.extend(parameters)
        return args, None
