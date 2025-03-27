import logging
import os
import re
import sys
import toml

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s][P%(process)s %(name)s:%(lineno)d][%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)


class Environment:
    def __init__(self):
        self.base_path = os.path.dirname(os.path.realpath(__file__))
        config_file_path = os.path.dirname(os.path.realpath(__file__))
        with open(os.path.join(config_file_path, "config.toml")) as f:
            self.config = toml.load(f)

    def update_airflow_config(self):
        """
        Load the Git airflow.cfg file and change path with our local airflow installation directory path.
        The new airflow.cfg file updated is written to our airflow file and Git version is not changed.
        """
        source_file = os.path.join(
            self.base_path, self.config["relative_paths"]["git_airflow_config"]
        )
        target_file = self.config["local_paths"]["airflow_config"]
        try:
            with open(source_file, "r") as file:
                data = file.read()
            pattern = r"(/\S+/airflow)"
            updated_config = re.sub(
                pattern,
                self.config["local_paths"]["airflow_install_dir"],
                data,
                flags=re.IGNORECASE,
            )

            new_sql_alchemy_value = "sqlite:///" + os.path.join(
                self.config["local_paths"]["airflow_install_dir"], "airflow.db"
            )
            updated_config = re.sub(
                r"(sql_alchemy_conn\s*=\s*)(.*)",
                rf"\1{new_sql_alchemy_value}",
                updated_config,
            )

            new_dags_folder = os.path.join(
                self.base_path.replace("environment", ""), "airflow", "dags"
            )
            updated_config = re.sub(
                r"(dags_folder\s*=\s*)(.*)", rf"\1{new_dags_folder}", updated_config
            )

            with open(target_file, "w") as file:
                file.write(updated_config)
            logging.info("airflow.cfg updated to local installation directory")
        except FileNotFoundError:
            logging.error(f"The file '{source_file}' does not exist.")
        except Exception as e:
            logging.error(f"[ERROR] An error occurred: {e}")


env = Environment()

__all__ = [env]
