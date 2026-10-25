"""Unit tests for the logical product mapping; no Azure/PostgreSQL required."""
import importlib.util
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "data_mesh" / "websocket_server.py"
spec = importlib.util.spec_from_file_location("iomt_websocket_server", MODULE_PATH)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

def test_supported_types_map_to_five_products():
    cases = {
        "smartwatch": "product-1",
        "smart_watch": "product-1",
        "bp_monitor": "product-2",
        "blood pressure monitor": "product-2",
        "glucose_monitor": "product-3",
        "ecg": "product-4",
        "pulse_oximeter": "product-5",
    }
    for device_type, product_id in cases.items():
        assert module.product_for_type(device_type) == product_id

def test_unknown_type_is_not_routed():
    assert module.product_for_type("unknown sensor") is None
