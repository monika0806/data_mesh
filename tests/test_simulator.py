import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "simulator" / "simulator.py"
spec = importlib.util.spec_from_file_location("iomt_simulator", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_registry_has_ten_unique_devices():
    devices = module.load_devices()
    assert len(devices) == 10
    ids = [d["registered_device_id"] for d in devices]
    assert len(ids) == len(set(ids))


def test_messages_include_routing_identifiers():
    device = module.load_devices()[0]
    message = module.make_message(device)
    assert message["data_mesh_id"] == device["data_mesh_id"]
    assert message["registered_device_id"] == device["registered_device_id"]
    assert message["device_type"] == device["device_type"]
    assert message["timestamp"]
    assert message["telemetry"]


def test_each_supported_device_generates_telemetry():
    for device in module.load_devices():
        assert isinstance(module.generate_telemetry(device["device_type"]), dict)
        assert module.generate_telemetry(device["device_type"])
