from .upgrade_manifest import set_config

def pytest_configure(config):
    set_config(config)
